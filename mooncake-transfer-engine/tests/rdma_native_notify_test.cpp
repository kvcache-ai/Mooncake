// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <optional>
#include <future>
#include <thread>
#include <unordered_set>

#include "config.h"
#include "error.h"
#include "transfer_engine.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/rdma_endpoint.h"

using namespace mooncake;
using Notify = TransferMetadata::NotifyDesc;

namespace mooncake {
class RdmaContextTestPeer {
   public:
    static void pauseNotifications(RdmaContext &context) {
        context.notify_running_.store(false, std::memory_order_release);
        if (context.notify_worker_.joinable()) context.notify_worker_.join();
    }
    static void drain(RdmaContext &context) {
        while (context.pollNotificationCq() > 0) {
        }
    }
    static void dispatch(RdmaContext &context, const ibv_wc &wc) {
        std::vector<Notify> received;
        context.dispatchNotificationCompletion(wc, received);
        EXPECT_TRUE(received.empty());
    }
    static void limitCapacity(RdmaContext &context, int entries) {
        std::lock_guard<std::mutex> guard(context.notify_mutex_);
        context.notify_max_cqe_ = entries;
    }
    static size_t reservations(RdmaContext &context) {
        std::lock_guard<std::mutex> guard(context.notify_mutex_);
        return context.notify_endpoints_.size();
    }
};
class RdmaNotificationTestPeer {
   public:
    static constexpr size_t kSlots = RdmaEndPoint::kNotifySlots;
    static constexpr size_t kSlotBytes = RdmaEndPoint::kNotifySlotBytes;
    static constexpr size_t kHeaderBytes = RdmaEndPoint::kNotifyHeaderBytes;
    static size_t encode(char *slot, const Notify &message) {
        return RdmaEndPoint::encodeNotification(slot, message);
    }
    static bool decode(const char *slot, size_t bytes, Notify &message) {
        return RdmaEndPoint::decodeNotification(slot, bytes, message);
    }
    static RdmaTransport *transport(TransferEngine &engine) {
        auto *t = engine.getTransport("rdma");
        if (!t) t = engine.getTransport("rdma_twosided");
        return dynamic_cast<RdmaTransport *>(t);
    }
    static bool hasNativeService(TransferEngine &engine) {
        auto *rdma = transport(engine);
        return rdma && !rdma->getContextList().empty() &&
               rdma->getContextList().front()->nativeNotifyEnabled();
    }
    static void stop(TransferEngine &engine) {
        auto *rdma = transport(engine);
        ASSERT_NE(rdma, nullptr);
        for (auto &context : rdma->getContextList())
            ASSERT_EQ(context->disconnectAllEndpoints(), 0);
    }
    static void restart(TransferEngine &engine) { stop(engine); }
    static std::shared_ptr<RdmaEndPoint> endpoint(TransferEngine &engine,
                                                  const std::string &peer) {
        auto *rdma = transport(engine);
        auto context = rdma->getContextList().front();
        auto desc = rdma->meta()->getSegmentDescByName(peer);
        if (!desc || desc->devices.empty()) return nullptr;
        return context->findEndpoint(
            MakeNicPath(desc->nicPathServerName(), desc->devices.front().name));
    }
    static uint32_t qpn(RdmaEndPoint &endpoint) {
        return endpoint.notificationQpNum();
    }
    static uint64_t generation(RdmaEndPoint &endpoint) {
        std::lock_guard<std::mutex> guard(endpoint.notify_.mutex);
        return endpoint.notify_.generation;
    }
    static ibv_cq *cq(RdmaEndPoint &endpoint) {
        std::lock_guard<std::mutex> guard(endpoint.notify_.mutex);
        return endpoint.notify_.qp ? endpoint.notify_.qp->send_cq : nullptr;
    }
    static void stop(RdmaEndPoint &endpoint) { endpoint.stopNotification(); }
    static void complete(RdmaEndPoint &endpoint, const ibv_wc &wc) {
        std::vector<Notify> received;
        endpoint.handleNotificationCompletion(wc, received);
        EXPECT_TRUE(received.empty());
    }
};
}  // namespace mooncake

namespace {
constexpr size_t kSlots = RdmaNotificationTestPeer::kSlots;
constexpr size_t kSlotBytes = RdmaNotificationTestPeer::kSlotBytes;
constexpr size_t kHeaderBytes = RdmaNotificationTestPeer::kHeaderBytes;
}  // namespace

TEST(NativeNotifyProtocol, BinaryAndBoundaryPayloads) {
    char frame[kSlotBytes];
    for (const auto &input : std::vector<Notify>{
             {"", ""},
             {std::string("a\0b", 3), std::string("\0\xff", 2)},
             {"name", std::string(kSlotBytes - kHeaderBytes - 4, 'x')}}) {
        size_t bytes = RdmaNotificationTestPeer::encode(frame, input);
        ASSERT_NE(bytes, 0u);
        Notify output;
        ASSERT_TRUE(RdmaNotificationTestPeer::decode(frame, bytes, output));
        EXPECT_EQ(output.name, input.name);
        EXPECT_EQ(output.notify_msg, input.notify_msg);
    }
    EXPECT_EQ(RdmaNotificationTestPeer::encode(
                  frame, {std::string(kSlotBytes, 'n'), ""}),
              0u);
    EXPECT_EQ(RdmaNotificationTestPeer::encode(
                  frame, {"", std::string(kSlotBytes, 'm')}),
              0u);
}

TEST(NativeNotifyProtocol, RejectsTruncatedAndMalformedFrames) {
    char frame[kSlotBytes];
    size_t bytes = RdmaNotificationTestPeer::encode(frame, {"name", "message"});
    Notify output;
    for (size_t n = 0; n < bytes; ++n)
        EXPECT_FALSE(RdmaNotificationTestPeer::decode(frame, n, output));
    EXPECT_FALSE(RdmaNotificationTestPeer::decode(frame, bytes + 1, output));
    EXPECT_FALSE(
        RdmaNotificationTestPeer::decode(frame, kSlotBytes + 1, output));
    frame[0] ^= 1;
    EXPECT_FALSE(RdmaNotificationTestPeer::decode(frame, bytes, output));
    RdmaNotificationTestPeer::encode(frame, {"name", "message"});
    uint32_t impossible = UINT32_MAX;
    std::memcpy(frame, &impossible, 4);
    EXPECT_FALSE(RdmaNotificationTestPeer::decode(frame, bytes, output));
}

namespace {
// Device discovery and bounded receive helper are shared in shape with the
// existing rdma_notify_test; that test and its transport stay unchanged.
bool rdmaDeviceUsable(ibv_device *device) {
    ibv_context *ctx = ibv_open_device(device);
    if (!ctx) return false;
    ibv_device_attr attr{};
    if (ibv_query_device(ctx, &attr) != 0) {
        ibv_close_device(ctx);
        return false;
    }
    bool ok = false;
    for (uint8_t port = 1; port <= attr.phys_port_cnt; ++port) {
        ibv_port_attr port_attr{};
        if (ibv_query_port(ctx, port, &port_attr) != 0) continue;
        if (port_attr.gid_tbl_len > 0 && port_attr.state == IBV_PORT_ACTIVE) {
            ok = true;
            break;
        }
    }
    ibv_close_device(ctx);
    return ok;
}

std::string pickRdmaDevice() {
    const char *override_name = std::getenv("MC_TEST_DEVICE_NAME");
    if (override_name && *override_name) return override_name;
    int num_devices = 0;
    ibv_device **list = ibv_get_device_list(&num_devices);
    if (!list || num_devices == 0) {
        if (list) ibv_free_device_list(list);
        return "";
    }
    std::string name;
    for (int i = 0; i < num_devices; ++i) {
        if (rdmaDeviceUsable(list[i])) {
            name = ibv_get_device_name(list[i]);
            break;
        }
    }
    ibv_free_device_list(list);
    return name;
}

bool waitForNotifies(TransferEngine &engine, size_t expect,
                     std::vector<TransferMetadata::NotifyDesc> &out,
                     int timeout_ms = 5000) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    out.clear();
    while (std::chrono::steady_clock::now() < deadline) {
        std::vector<TransferMetadata::NotifyDesc> batch;
        engine.getNotifies(batch);
        out.insert(out.end(), batch.begin(), batch.end());
        if (out.size() >= expect) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return out.size() >= expect;
}

}  // namespace

class NativeNotifyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        original_ = globalConfig();
        device_ = pickRdmaDevice();
        if (device_.empty()) GTEST_SKIP() << "no usable RDMA device";
        for (const auto &entry :
             std::vector<std::pair<std::string, std::string>>{
                 {"MC_TE_FILTERS", device_},
                 {"MC_USE_RDMA_TWOSIDED", "0"},
                 {"MC_RDMA_NOTIFY_ENABLED", "1"},
                 {"MC_RDMA_NOTIFY_OOB_FALLBACK", "0"}}) {
            const char *previous = std::getenv(entry.first.c_str());
            env_.push_back(
                {entry.first, previous ? std::optional<std::string>(previous)
                                       : std::nullopt});
            setenv(entry.first.c_str(), entry.second.c_str(), 1);
        }
        loadGlobalConfig(globalConfig());
        sender_ = makeEngine("127.0.0.1:0");
        receiver_ = makeEngine("127.0.0.1:0");
        ASSERT_NE(sender_, nullptr);
        ASSERT_NE(receiver_, nullptr);
        sender_name_ = sender_->getLocalIpAndPort();
        receiver_name_ = receiver_->getLocalIpAndPort();
    }
    std::unique_ptr<TransferEngine> makeEngine(const std::string &name) {
        auto engine = std::make_unique<TransferEngine>(
            true, std::vector<std::string>{device_});
        if (engine->init("P2PHANDSHAKE", name)) return nullptr;
        return engine;
    }
    void TearDown() override {
        sender_.reset();
        receiver_.reset();
        for (const auto &entry : env_) {
            if (entry.second)
                setenv(entry.first.c_str(), entry.second->c_str(), 1);
            else
                unsetenv(entry.first.c_str());
        }
        globalConfig() = original_;
    }
    GlobalConfig original_;
    std::vector<std::pair<std::string, std::optional<std::string>>> env_;
    std::string device_, sender_name_, receiver_name_;
    // Keep registered buffers alive until TearDown destroys both engines,
    // including when an assertion interrupts a transfer test.
    std::vector<char> source_, target_;
    std::unique_ptr<TransferEngine> sender_, receiver_;
    void prepareBuffers() {
        source_.assign(4096, 'a');
        target_.assign(4096, 0);
        ASSERT_EQ(sender_->registerLocalMemory(source_.data(), source_.size()),
                  0);
        ASSERT_EQ(
            receiver_->registerLocalMemory(target_.data(), target_.size()), 0);
    }
    void transfer(TransferRequest::OpCode opcode, bool notify = false) {
        auto segment = sender_->openSegment(receiver_name_);
        ASSERT_NE(segment, static_cast<SegmentID>(-1));
        TransferRequest request{};
        request.opcode = opcode;
        request.source = source_.data();
        request.target_id = segment;
        request.target_offset = reinterpret_cast<uint64_t>(target_.data());
        request.length = source_.size();
        auto batch = sender_->allocateBatchID(1);
        ASSERT_TRUE((notify ? sender_->submitTransferWithNotify(
                                  batch, {request}, {"batch", "done"})
                            : sender_->submitTransfer(batch, {request}))
                        .ok());
        TransferStatus status{};
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        do {
            ASSERT_TRUE(sender_->getTransferStatus(batch, 0, status).ok());
            ASSERT_NE(status.s, TransferStatusEnum::FAILED);
            if (status.s == TransferStatusEnum::COMPLETED) break;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
        ASSERT_TRUE(sender_->freeBatchID(batch).ok());
        EXPECT_EQ(source_, target_);
    }
};

TEST_F(NativeNotifyTest, DataAndNotificationsReuseEndpoint) {
    ASSERT_NO_FATAL_FAILURE(prepareBuffers());
    ASSERT_NO_FATAL_FAILURE(transfer(TransferRequest::WRITE));
    auto endpoint =
        RdmaNotificationTestPeer::endpoint(*sender_, receiver_name_);
    ASSERT_NE(endpoint, nullptr);
    auto qpn = RdmaNotificationTestPeer::qpn(*endpoint);
    ASSERT_NE(qpn, 0u);
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"explicit", "done"}),
              0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    std::fill(target_.begin(), target_.end(), 'b');
    ASSERT_NO_FATAL_FAILURE(transfer(TransferRequest::READ, true));
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].name, "batch");
    EXPECT_EQ(RdmaNotificationTestPeer::endpoint(*sender_, receiver_name_),
              endpoint);
    EXPECT_EQ(RdmaNotificationTestPeer::qpn(*endpoint), qpn);
}

TEST_F(NativeNotifyTest, LocalNotificationFailureDoesNotResetDataQps) {
    ASSERT_NO_FATAL_FAILURE(prepareBuffers());
    ASSERT_NO_FATAL_FAILURE(transfer(TransferRequest::WRITE));
    auto endpoint =
        RdmaNotificationTestPeer::endpoint(*sender_, receiver_name_);
    ASSERT_NE(endpoint, nullptr);
    // A local channel failure is sticky. It must not replay via TCP or evict
    // a healthy data connection merely because a notification was requested.
    RdmaNotificationTestPeer::stop(*endpoint);
    globalConfig().rdma_notify_oob_fallback = true;
    EXPECT_EQ(sender_->sendNotifyByName(receiver_name_, {"failed", "x"}),
              ERR_ENDPOINT);
    EXPECT_EQ(RdmaNotificationTestPeer::endpoint(*sender_, receiver_name_),
              endpoint);
    EXPECT_TRUE(endpoint->readyToSend());
    std::fill(source_.begin(), source_.end(), 'c');
    ASSERT_NO_FATAL_FAILURE(transfer(TransferRequest::WRITE));
    std::vector<Notify> got;
    receiver_->getNotifies(got);
    EXPECT_TRUE(got.empty());
}

TEST_F(NativeNotifyTest, RetirementWakesSenderWaitingForSlot) {
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"setup", "x"}), 0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    auto endpoint =
        RdmaNotificationTestPeer::endpoint(*sender_, receiver_name_);
    ASSERT_NE(endpoint, nullptr);
    auto context =
        RdmaNotificationTestPeer::transport(*sender_)->getContextList().front();
    RdmaContextTestPeer::pauseNotifications(*context);
    // Drain the setup send before suspending CQ progress and filling every
    // slot.
    RdmaContextTestPeer::drain(*context);
    for (size_t i = 0; i < kSlots; ++i)
        ASSERT_EQ(endpoint->sendNotification({"full", std::to_string(i)}), 0);
    auto blocked = std::async(std::launch::async, [&] {
        return sender_->sendNotifyByName(receiver_name_, {"blocked", "x"});
    });
    EXPECT_EQ(blocked.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    context->deleteEndpointByPtr(endpoint.get());
    EXPECT_EQ(blocked.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(blocked.get(), ERR_ENDPOINT);
}

TEST_F(NativeNotifyTest, DefaultSettingsEnableNativeNotifications) {
    sender_.reset();
    receiver_.reset();
    unsetenv("MC_RDMA_NOTIFY_ENABLED");
    unsetenv("MC_RDMA_NOTIFY_OOB_FALLBACK");
    GlobalConfig defaults;
    globalConfig().rdma_notify_enabled = defaults.rdma_notify_enabled;
    globalConfig().rdma_notify_oob_fallback = defaults.rdma_notify_oob_fallback;
    loadGlobalConfig(globalConfig());
    sender_ = makeEngine("127.0.0.1:0");
    receiver_ = makeEngine("127.0.0.1:0");
    ASSERT_NE(sender_, nullptr);
    ASSERT_NE(receiver_, nullptr);
    ASSERT_TRUE(RdmaNotificationTestPeer::hasNativeService(*sender_));
    ASSERT_TRUE(RdmaNotificationTestPeer::hasNativeService(*receiver_));
    // Disable fallback for this send so a TCP success cannot mask bad routing.
    globalConfig().rdma_notify_oob_fallback = false;
    ASSERT_EQ(sender_->sendNotifyByName(receiver_->getLocalIpAndPort(),
                                        {"default", "rdma"}),
              0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].notify_msg, "rdma");
}

TEST_F(NativeNotifyTest, SharedSwitchDisablesNativeServiceAndUsesTcp) {
    sender_.reset();
    receiver_.reset();
    setenv("MC_RDMA_NOTIFY_ENABLED", "0", 1);
    loadGlobalConfig(globalConfig());
    sender_ = makeEngine("127.0.0.1:0");
    receiver_ = makeEngine("127.0.0.1:0");
    ASSERT_NE(sender_, nullptr);
    ASSERT_NE(receiver_, nullptr);
    EXPECT_FALSE(RdmaNotificationTestPeer::hasNativeService(*sender_));
    EXPECT_FALSE(RdmaNotificationTestPeer::hasNativeService(*receiver_));
    // TCP is the selected path, even with RDMA fallback disabled. A payload
    // larger than the native slot also catches accidental RDMA dispatch.
    Notify message{"disabled", std::string(kSlotBytes, 't')};
    ASSERT_EQ(
        sender_->sendNotifyByName(receiver_->getLocalIpAndPort(), message), 0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].notify_msg, message.notify_msg);
}

TEST_F(NativeNotifyTest, TwosidedKeepsItsOwnNotificationService) {
    sender_.reset();
    receiver_.reset();
    setenv("MC_USE_RDMA_TWOSIDED", "1", 1);
    loadGlobalConfig(globalConfig());
    sender_ = makeEngine("127.0.0.1:0");
    receiver_ = makeEngine("127.0.0.1:0");
    ASSERT_NE(sender_, nullptr);
    ASSERT_NE(receiver_, nullptr);
    ASSERT_NE(sender_->getTransport("rdma_twosided"), nullptr);
    ASSERT_NE(receiver_->getTransport("rdma_twosided"), nullptr);
    EXPECT_FALSE(RdmaNotificationTestPeer::hasNativeService(*sender_));
    EXPECT_FALSE(RdmaNotificationTestPeer::hasNativeService(*receiver_));
    ASSERT_EQ(sender_->sendNotifyByName(receiver_->getLocalIpAndPort(),
                                        {"twosided", "ctrl"}),
              0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].notify_msg, "ctrl");
}

TEST_F(NativeNotifyTest, FullSlotRoundTrip) {
    Notify message{"binary",
                   std::string(kSlotBytes - kHeaderBytes - 6, '\xff')};
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, message), 0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].name, message.name);
    EXPECT_EQ(got[0].notify_msg, message.notify_msg);
}

TEST_F(NativeNotifyTest, ConcurrentBurstReusesSlotsWithoutDuplicates) {
    constexpr int kThreads = 6, kPerThread = 256;
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&, t] {
            for (int n = 0; n < kPerThread; ++n) {
                auto id = std::to_string(t * kPerThread + n);
                if (sender_->sendNotifyByName(receiver_name_,
                                              {id, id + std::string(200, 'x')}))
                    ++errors;
            }
        });
    for (auto &thread : threads) thread.join();
    ASSERT_EQ(errors.load(), 0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, kThreads * kPerThread, got));
    ASSERT_EQ(got.size(), kThreads * kPerThread);
    std::unordered_set<std::string> unique;
    for (const auto &msg : got) {
        EXPECT_EQ(msg.notify_msg, msg.name + std::string(200, 'x'));
        EXPECT_TRUE(unique.insert(msg.name).second);
    }
}

TEST_F(NativeNotifyTest, SimultaneousFirstSendInBothDirections) {
    std::atomic<int> started{0}, errors{0};
    auto send = [&](TransferEngine &engine, const std::string &peer) {
        ++started;
        while (started.load() != 2) std::this_thread::yield();
        for (int i = 0; i < 256; ++i)
            if (engine.sendNotifyByName(peer,
                                        {"bidirectional", std::to_string(i)}))
                ++errors;
    };
    std::thread a(send, std::ref(*sender_), std::cref(receiver_name_));
    std::thread b(send, std::ref(*receiver_), std::cref(sender_name_));
    a.join();
    b.join();
    ASSERT_EQ(errors.load(), 0);
    for (auto *engine : {sender_.get(), receiver_.get()}) {
        std::vector<Notify> got;
        ASSERT_TRUE(waitForNotifies(*engine, 256, got));
        ASSERT_EQ(got.size(), 256u);
        for (int i = 0; i < 256; ++i)
            EXPECT_EQ(got[i].notify_msg, std::to_string(i));
    }
}

TEST_F(NativeNotifyTest, LoopbackDoesNotDeadlock) {
    for (int i = 0; i < 128; ++i)
        ASSERT_EQ(sender_->sendNotifyByName(sender_name_,
                                            {"self", std::to_string(i)}),
                  0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*sender_, 128, got));
    ASSERT_EQ(got.size(), 128u);
}

TEST_F(NativeNotifyTest, DisabledPeerFallsBackOnlyWhenAllowed) {
    receiver_.reset();
    globalConfig().rdma_notify_enabled = false;
    receiver_ = makeEngine("127.0.0.1:0");
    ASSERT_NE(receiver_, nullptr);
    receiver_name_ = receiver_->getLocalIpAndPort();
    globalConfig().rdma_notify_enabled = true;
    EXPECT_EQ(sender_->sendNotifyByName(receiver_name_, {"strict", "x"}),
              ERR_NOT_IMPLEMENTED);
    std::vector<Notify> got;
    receiver_->getNotifies(got);
    EXPECT_TRUE(got.empty());
    globalConfig().rdma_notify_oob_fallback = true;
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"fallback", "tcp"}),
              0);
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].name, "fallback");
}

TEST_F(NativeNotifyTest, OversizedPayloadFallsBackOnlyWhenAllowed) {
    Notify large{"large", std::string(kSlotBytes, 'x')};
    EXPECT_EQ(sender_->sendNotifyByName(receiver_name_, large),
              ERR_NOT_IMPLEMENTED);
    globalConfig().rdma_notify_oob_fallback = true;
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, large), 0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].notify_msg, large.notify_msg);
}

// P2PHANDSHAKE init always allocates a new TCP port, even if the caller passes
// the old port. Retire the endpoint while preserving the RPC listener/context
// so these tests exercise fresh data and notification QPs for the same peer
// identity.
TEST_F(NativeNotifyTest, SenderEndpointRestartReusesIdentity) {
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"before", "x"}), 0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    RdmaNotificationTestPeer::restart(*sender_);
    ASSERT_EQ(sender_->getLocalIpAndPort(), sender_name_);
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"after", "y"}), 0);
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].name, "after");
}

TEST_F(NativeNotifyTest, ReceiverEndpointRestartReportsFailureThenReconnects) {
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"before", "x"}), 0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    RdmaNotificationTestPeer::stop(*receiver_);
    bool failed = false;
    for (int i = 0; i < 100 && !failed; ++i) {
        failed =
            sender_->sendNotifyByName(receiver_name_, {"lost-peer", "x"}) != 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(failed);
    RdmaNotificationTestPeer::restart(*receiver_);
    ASSERT_EQ(receiver_->getLocalIpAndPort(), receiver_name_);
    int ret = ERR_ENDPOINT;
    auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds(globalConfig().conn_pause_ttl_ms + 3000);
    do {
        ret = sender_->sendNotifyByName(receiver_name_, {"after", "y"});
        if (!ret) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < deadline);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].name, "after");
}

TEST_F(NativeNotifyTest, ConcurrentUnreachablePeerIsBounded) {
    auto start = std::chrono::steady_clock::now();
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i)
        threads.emplace_back([&] {
            if (sender_->sendNotifyByName("127.0.0.1:19998", {"missing", "x"}))
                ++failures;
        });
    for (auto &thread : threads) thread.join();
    EXPECT_EQ(failures.load(), 8);
    EXPECT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(15));
}

TEST_F(NativeNotifyTest, SharedCqGrowsAndRoutesConcurrentPeers) {
    // Start small to exercise real ibv_resize_cq with three endpoint queues.
    globalConfig().max_cqe = 2 * kSlots;
    receiver_.reset();
    receiver_ = makeEngine("127.0.0.1:0");
    ASSERT_NE(receiver_, nullptr);
    receiver_name_ = receiver_->getLocalIpAndPort();
    auto second = makeEngine("127.0.0.1:0");
    auto third = makeEngine("127.0.0.1:0");
    ASSERT_NE(second, nullptr);
    ASSERT_NE(third, nullptr);
    std::vector<TransferEngine *> peers{sender_.get(), second.get(),
                                        third.get()};
    std::vector<std::shared_ptr<RdmaEndPoint>> endpoints;
    for (auto *peer : peers) {
        ASSERT_EQ(peer->sendNotifyByName(receiver_name_, {"setup", "x"}), 0);
        endpoints.push_back(RdmaNotificationTestPeer::endpoint(
            *receiver_, peer->getLocalIpAndPort()));
        ASSERT_NE(endpoints.back(), nullptr);
    }
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, peers.size(), got));
    auto *cq = RdmaNotificationTestPeer::cq(*endpoints.front());
    ASSERT_NE(cq, nullptr);
    EXPECT_GE(cq->cqe, peers.size() * 2 * kSlots);
    for (const auto &endpoint : endpoints)
        EXPECT_EQ(RdmaNotificationTestPeer::cq(*endpoint), cq);
    auto outgoing =
        RdmaNotificationTestPeer::endpoint(*sender_, receiver_name_);
    ASSERT_NE(outgoing, nullptr);
    EXPECT_NE(RdmaNotificationTestPeer::cq(*outgoing), cq);
    constexpr size_t kMessages = 512;
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;
    for (size_t p = 0; p < peers.size(); ++p)
        threads.emplace_back([&, p] {
            for (size_t i = 0; i < kMessages; ++i) {
                auto id = std::to_string(p) + ":" + std::to_string(i);
                if (peers[p]->sendNotifyByName(receiver_name_, {id, id}))
                    ++errors;
            }
        });
    for (auto &thread : threads) thread.join();
    ASSERT_EQ(errors.load(), 0);
    ASSERT_TRUE(waitForNotifies(*receiver_, peers.size() * kMessages, got));
    ASSERT_EQ(got.size(), peers.size() * kMessages);
    std::unordered_set<std::string> ids;
    for (const auto &message : got) {
        EXPECT_EQ(message.name, message.notify_msg);
        EXPECT_TRUE(ids.insert(message.name).second);
    }
    // Retiring one QP must not destroy its siblings' CQ or poison their sends.
    auto context = RdmaNotificationTestPeer::transport(*receiver_)
                       ->getContextList()
                       .front();
    context->deleteEndpointByPtr(endpoints.front().get());
    context->reclaimEndpoints();
    for (size_t p = 1; p < peers.size(); ++p) {
        EXPECT_EQ(RdmaNotificationTestPeer::cq(*endpoints[p]), cq);
        ASSERT_EQ(
            peers[p]->sendNotifyByName(receiver_name_, {"survivor", "ok"}), 0);
    }
    ASSERT_TRUE(waitForNotifies(*receiver_, peers.size() - 1, got));
    EXPECT_EQ(got.size(), peers.size() - 1);
}

TEST_F(NativeNotifyTest, StaleCompletionsCannotAffectReplacementEndpoint) {
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"before", "x"}), 0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    auto context =
        RdmaNotificationTestPeer::transport(*sender_)->getContextList().front();
    auto old = RdmaNotificationTestPeer::endpoint(*sender_, receiver_name_);
    ASSERT_NE(old, nullptr);
    const auto generation = RdmaNotificationTestPeer::generation(*old);
    RdmaContextTestPeer::pauseNotifications(*context);
    RdmaContextTestPeer::drain(*context);
    context->deleteEndpointByPtr(old.get());
    context->reclaimEndpoints();
    ASSERT_EQ(RdmaNotificationTestPeer::qpn(*old), 0u);
    // Reservations are released only once the shared CQ has been drained.
    EXPECT_EQ(RdmaContextTestPeer::reservations(*context), 1u);
    RdmaContextTestPeer::drain(*context);
    EXPECT_EQ(RdmaContextTestPeer::reservations(*context), 0u);
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"after", "y"}), 0);
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    auto current = RdmaNotificationTestPeer::endpoint(*sender_, receiver_name_);
    ASSERT_NE(current, nullptr);
    ASSERT_NE(current, old);
    EXPECT_NE(RdmaNotificationTestPeer::generation(*current), generation);
    ibv_wc stale{};
    stale.wr_id = generation * kSlots;
    // Simulate hardware reusing a QPN while an old CQE is awaiting dispatch.
    stale.qp_num = RdmaNotificationTestPeer::qpn(*current);
    stale.status = IBV_WC_LOC_LEN_ERR;
    RdmaContextTestPeer::dispatch(*context, stale);
    // Also cover an already-resolved callback racing QP reconstruction.
    RdmaNotificationTestPeer::complete(*current, stale);
    EXPECT_FALSE(current->notificationNeedsReconnect());
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"unaffected", "z"}),
              0);
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got.front().name, "unaffected");
}

TEST_F(NativeNotifyTest, CqCapacityFailurePreservesExistingEndpoint) {
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"setup", "x"}), 0);
    std::vector<Notify> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    auto context = RdmaNotificationTestPeer::transport(*receiver_)
                       ->getContextList()
                       .front();
    RdmaContextTestPeer::limitCapacity(*context, 2 * kSlots);
    auto extra = makeEngine("127.0.0.1:0");
    ASSERT_NE(extra, nullptr);
    EXPECT_EQ(extra->sendNotifyByName(receiver_name_, {"no-room", "x"}),
              ERR_NOT_IMPLEMENTED);
    // Resource failure stays an error, even when TCP fallback is allowed.
    globalConfig().rdma_notify_oob_fallback = true;
    EXPECT_EQ(receiver_->sendNotifyByName(extra->getLocalIpAndPort(),
                                          {"failed-init", "x"}),
              ERR_ENDPOINT);
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, {"survivor", "ok"}), 0);
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got.front().name, "survivor");
}

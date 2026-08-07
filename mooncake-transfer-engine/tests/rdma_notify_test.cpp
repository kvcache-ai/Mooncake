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
//
// Correctness + lightweight perf checks for RDMA CtrlChannel notify path.
// Requires an RDMA device. Self-skips when none is present.
//
// Env:
//   MC_TE_FILTERS / MC_TEST_DEVICE_NAME — pin NIC (default: first ibv device)
//   MC_RDMA_NOTIFY_OOB_FALLBACK=0        — force RDMA path (set by this test)

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <infiniband/verbs.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "config.h"
#include "transfer_engine.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

namespace {

std::string pickRdmaDevice() {
    const char *override_name = std::getenv("MC_TEST_DEVICE_NAME");
    if (override_name && *override_name) return override_name;
    int num_devices = 0;
    ibv_device **list = ibv_get_device_list(&num_devices);
    if (!list || num_devices == 0) {
        if (list) ibv_free_device_list(list);
        return "";
    }
    std::string name = ibv_get_device_name(list[0]);
    ibv_free_device_list(list);
    return name;
}

bool waitForNotifies(TransferEngine &engine, size_t expect,
                     std::vector<TransferMetadata::NotifyDesc> &out,
                     int timeout_ms = 5000) {
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
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

class RdmaNotifyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        device_ = pickRdmaDevice();
        if (device_.empty()) {
            GTEST_SKIP() << "no RDMA device available";
        }
        ASSERT_EQ(setenv("MC_TE_FILTERS", device_.c_str(), 1), 0);
        ASSERT_EQ(setenv("MC_RDMA_NOTIFY_ENABLED", "1", 1), 0);
        ASSERT_EQ(setenv("MC_RDMA_NOTIFY_OOB_FALLBACK", "0", 1), 0);
        loadGlobalConfig(globalConfig());
        ASSERT_TRUE(globalConfig().rdma_notify_enabled);
        ASSERT_FALSE(globalConfig().rdma_notify_oob_fallback);

        std::vector<std::string> filter{device_};
        sender_ = std::make_unique<TransferEngine>(true, filter);
        receiver_ = std::make_unique<TransferEngine>(true, filter);
        ASSERT_EQ(sender_->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
        ASSERT_EQ(receiver_->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
        sender_name_ = sender_->getLocalIpAndPort();
        receiver_name_ = receiver_->getLocalIpAndPort();
        ASSERT_FALSE(sender_name_.empty());
        ASSERT_FALSE(receiver_name_.empty());
        ASSERT_NE(sender_->getTransport("rdma"), nullptr);
        ASSERT_NE(receiver_->getTransport("rdma"), nullptr);
    }

    void TearDown() override {
        sender_.reset();
        receiver_.reset();
        unsetenv("MC_TE_FILTERS");
        unsetenv("MC_RDMA_NOTIFY_ENABLED");
        unsetenv("MC_RDMA_NOTIFY_OOB_FALLBACK");
        loadGlobalConfig(globalConfig());
    }

    std::string device_;
    std::unique_ptr<TransferEngine> sender_;
    std::unique_ptr<TransferEngine> receiver_;
    std::string sender_name_;
    std::string receiver_name_;
};

TEST_F(RdmaNotifyTest, SingleNotifyCorrectness) {
    TransferMetadata::NotifyDesc msg;
    msg.name = "hello";
    msg.notify_msg = "world-from-rdma-ctrl";

    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, msg), 0);

    std::vector<TransferMetadata::NotifyDesc> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got))
        << "timed out waiting for RDMA notify";
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].name, msg.name);
    EXPECT_EQ(got[0].notify_msg, msg.notify_msg);
}

TEST_F(RdmaNotifyTest, BidirectionalNotify) {
    TransferMetadata::NotifyDesc a2b{"a2b", "ping"};
    TransferMetadata::NotifyDesc b2a{"b2a", "pong"};

    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, a2b), 0);
    ASSERT_EQ(receiver_->sendNotifyByName(sender_name_, b2a), 0);

    std::vector<TransferMetadata::NotifyDesc> recv_a, recv_b;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, recv_a));
    ASSERT_TRUE(waitForNotifies(*sender_, 1, recv_b));
    EXPECT_EQ(recv_a[0].name, "a2b");
    EXPECT_EQ(recv_a[0].notify_msg, "ping");
    EXPECT_EQ(recv_b[0].name, "b2a");
    EXPECT_EQ(recv_b[0].notify_msg, "pong");
}

TEST_F(RdmaNotifyTest, BurstNotifyCorrectness) {
    constexpr int kCount = 128;

    for (int i = 0; i < kCount; ++i) {
        TransferMetadata::NotifyDesc msg;
        msg.name = "n" + std::to_string(i);
        msg.notify_msg = "payload-" + std::to_string(i);
        ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, msg), 0)
            << "failed at i=" << i;
    }

    std::vector<TransferMetadata::NotifyDesc> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, kCount, got, 10000));
    ASSERT_EQ(got.size(), static_cast<size_t>(kCount));
    for (int i = 0; i < kCount; ++i) {
        EXPECT_EQ(got[i].name, "n" + std::to_string(i));
        EXPECT_EQ(got[i].notify_msg, "payload-" + std::to_string(i));
    }
}

TEST_F(RdmaNotifyTest, SessionOpenAndCreditGrant) {
    // Trigger CtrlChannel connect via a notify; SESSION_OPEN/GRANT follow.
    TransferMetadata::NotifyDesc msg{"bootstrap", "x"};
    ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, msg), 0);

    std::vector<TransferMetadata::NotifyDesc> got;
    ASSERT_TRUE(waitForNotifies(*receiver_, 1, got));

    auto *sender_rdma =
        dynamic_cast<RdmaTransport *>(sender_->getTransport("rdma"));
    auto *receiver_rdma =
        dynamic_cast<RdmaTransport *>(receiver_->getTransport("rdma"));
    ASSERT_NE(sender_rdma, nullptr);
    ASSERT_NE(receiver_rdma, nullptr);

    // Wait until both directions have applied CREDIT_GRANT bounce slots.
    uint64_t sender_slots = 0, receiver_slots = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        sender_slots = sender_rdma->peerGrantedBounceSlots(receiver_name_);
        receiver_slots = receiver_rdma->peerGrantedBounceSlots(sender_name_);
        if (sender_slots > 0 && receiver_slots > 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(sender_slots, globalConfig().rdma_msg_pool_base)
        << "sender missing grant from receiver";
    EXPECT_EQ(receiver_slots, globalConfig().rdma_msg_pool_base)
        << "receiver missing grant from sender";

    auto &ledger = sender_rdma->senderCreditLedger();
    const uint64_t session = receiver_rdma->localCtrlSessionId();
    ASSERT_EQ(ledger.tryReserve(
                  receiver_name_, session,
                  {{CreditResource::BounceSlots, 1},
                   {CreditResource::BounceBytes,
                    globalConfig().rdma_msg_slot_size}}),
              0);
}

TEST_F(RdmaNotifyTest, NotifyThroughputSmoke) {
    constexpr int kWarmup = 32;
    constexpr int kIters = 2000;

    TransferMetadata::NotifyDesc msg{"perf", "x"};
    for (int i = 0; i < kWarmup; ++i) {
        ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, msg), 0);
    }
    std::vector<TransferMetadata::NotifyDesc> drain;
    ASSERT_TRUE(waitForNotifies(*receiver_, kWarmup, drain, 10000));

    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < kIters; ++i) {
        ASSERT_EQ(sender_->sendNotifyByName(receiver_name_, msg), 0);
    }
    ASSERT_TRUE(waitForNotifies(*receiver_, kIters, drain, 15000));
    auto t1 = std::chrono::steady_clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double ops_per_sec = kIters * 1000.0 / ms;
    double avg_us = ms * 1000.0 / kIters;
    LOG(INFO) << "RDMA notify perf: iters=" << kIters << " wall_ms=" << ms
              << " avg_us=" << avg_us << " ops_per_sec=" << ops_per_sec;
    EXPECT_GT(ops_per_sec, 200.0);
}

TEST(RdmaNotifyOobTest, OobFallbackStillWorksWhenRdmaDisabled) {
    std::string device = pickRdmaDevice();
    if (device.empty()) {
        GTEST_SKIP() << "no RDMA device available";
    }
    ASSERT_EQ(setenv("MC_TE_FILTERS", device.c_str(), 1), 0);
    ASSERT_EQ(setenv("MC_RDMA_NOTIFY_ENABLED", "0", 1), 0);
    ASSERT_EQ(setenv("MC_RDMA_NOTIFY_OOB_FALLBACK", "1", 1), 0);
    loadGlobalConfig(globalConfig());
    ASSERT_FALSE(globalConfig().rdma_notify_enabled);

    std::vector<std::string> filter{device};
    auto sender = std::make_unique<TransferEngine>(true, filter);
    auto receiver = std::make_unique<TransferEngine>(true, filter);
    ASSERT_EQ(sender->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
    ASSERT_EQ(receiver->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);

    TransferMetadata::NotifyDesc msg{"oob", "fallback"};
    ASSERT_EQ(sender->sendNotifyByName(receiver->getLocalIpAndPort(), msg), 0);

    std::vector<TransferMetadata::NotifyDesc> got;
    ASSERT_TRUE(waitForNotifies(*receiver, 1, got));
    EXPECT_EQ(got[0].name, "oob");
    EXPECT_EQ(got[0].notify_msg, "fallback");

    sender.reset();
    receiver.reset();
    unsetenv("MC_TE_FILTERS");
    unsetenv("MC_RDMA_NOTIFY_ENABLED");
    unsetenv("MC_RDMA_NOTIFY_OOB_FALLBACK");
    loadGlobalConfig(globalConfig());
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/control_plane.h"
#include "tent/runtime/receiver_credit_controller.h"

#include <chrono>
#include <limits>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {

constexpr size_t index(CreditResource resource) {
    return static_cast<size_t>(resource) - 1;
}

ReceiverCreditRuntimeConfig controllerConfig(CreditRolloutMode mode) {
    ReceiverCreditRuntimeConfig config;
    config.mode = mode;
    config.max_peers = 16;
    config.max_grant_per_pull[index(CreditResource::DataBytes)] = 8192;
    config.max_grant_per_pull[index(CreditResource::RequestSlots)] = 4;
    return config;
}

std::shared_ptr<ReceiverCreditAllocator> allocator() {
    ReceiverCreditAllocatorConfig config;
    config.capacity[index(CreditResource::DataBytes)] = 1ULL << 20;
    config.capacity[index(CreditResource::RequestSlots)] = 64;
    config.max_grant_per_pull[index(CreditResource::DataBytes)] = 8192;
    config.max_grant_per_pull[index(CreditResource::RequestSlots)] = 4;
    config.max_entries = 16;
    config.ttl_ms = 1000;
    config.retry_after_us = 100;
    config.receiver_session_id = {11, 22};
    config.epoch = 1;
    std::unique_ptr<ReceiverCreditAllocator> result;
    EXPECT_TRUE(ReceiverCreditAllocator::create(config, result).ok());
    return std::shared_ptr<ReceiverCreditAllocator>(std::move(result));
}

CreditCharge charge() {
    return {
        {{CreditResource::DataBytes, 4096}, {CreditResource::RequestSlots, 1}}};
}

template <typename Predicate>
bool waitUntil(Predicate predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return predicate();
}

TEST(ReceiverCreditController, PullActivatesLedgerAndReportsCompletion) {
    ControlService service("p2p", "", nullptr);
    service.setReceiverCreditAllocator(allocator());
    uint16_t port = 0;
    ASSERT_TRUE(service.start(port).ok());
    const std::string address = "127.0.0.1:" + std::to_string(port);

    auto contexts = std::make_shared<CreditPeerContextTable>(16);
    auto ledger = std::make_shared<SenderCreditLedger>(16);
    std::shared_ptr<ReceiverCreditPullController> controller;
    ASSERT_TRUE(ReceiverCreditPullController::create(
                    controllerConfig(CreditRolloutMode::Required), 77, contexts,
                    ledger, controller)
                    .ok());
    ASSERT_TRUE(controller->request(9, address, 0, charge()).ok());

    CreditPeerContextSnapshot peer;
    ASSERT_TRUE(
        waitUntil([&] { return contexts->lookupFresh(9, 0, peer).ok(); }));
    uint64_t available = 0;
    ASSERT_TRUE(
        ledger->available(peer.key, CreditResource::DataBytes, available).ok());
    EXPECT_EQ(available, 8192);

    ReceiverCreditDispatchGate gate(*contexts, *ledger);
    CreditDispatchSnapshot snapshot;
    ASSERT_TRUE(gate.snapshot(9, 0, charge(), snapshot).ok());
    CreditDispatchReservation reservation;
    ASSERT_TRUE(gate.tryReserve(snapshot, reservation).ok());
    ASSERT_TRUE(gate.commit(reservation).ok());
    ASSERT_TRUE(gate.release(reservation).ok());
    ASSERT_TRUE(controller->request(9, address, 0, charge()).ok());

    ASSERT_TRUE(waitUntil([&] {
        CreditLedgerSnapshot usage;
        return ledger->snapshot(peer.key, peer.epoch, usage).ok() &&
               usage.grants[index(CreditResource::DataBytes)] == 12288;
    }));
    controller->stop();
}

TEST(ReceiverCreditController, ExplicitUnsupportedObeysRolloutMode) {
    ControlService service("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(service.start(port).ok());
    const std::string address = "127.0.0.1:" + std::to_string(port);

    for (auto [mode, expected] :
         {std::pair{CreditRolloutMode::Optional, CreditPeerState::Legacy},
          std::pair{CreditRolloutMode::Required, CreditPeerState::Failed}}) {
        auto contexts = std::make_shared<CreditPeerContextTable>(16);
        auto ledger = std::make_shared<SenderCreditLedger>(16);
        std::shared_ptr<ReceiverCreditPullController> controller;
        ASSERT_TRUE(ReceiverCreditPullController::create(controllerConfig(mode),
                                                         77, contexts, ledger,
                                                         controller)
                        .ok());
        ASSERT_TRUE(controller->request(9, address, 0, charge()).ok());
        ASSERT_TRUE(
            waitUntil([&] { return controller->peerState(9, 0) == expected; }));
        controller->stop();
    }
}

TEST(AdaptiveCreditDispatchLimiter,
     SlowPullReducesAndNeverReprobesLearnedUnsafeLevel) {
    auto config = controllerConfig(CreditRolloutMode::Required);
    config.adaptive_dispatch_min_owners = 1;
    config.adaptive_dispatch_initial_owners = 4;
    config.adaptive_dispatch_max_owners = 8;
    config.adaptive_dispatch_slow_rtt_us = 1000;
    config.adaptive_dispatch_healthy_pulls = 2;
    AdaptiveCreditDispatchLimiter limiter(config);

    limiter.observe(std::chrono::microseconds(100), true, 4);
    limiter.observe(std::chrono::microseconds(100), true, 4);
    ASSERT_EQ(limiter.ownerLimit(), 5);

    limiter.observe(std::chrono::milliseconds(200), true, 5);
    EXPECT_EQ(limiter.ownerLimit(), 2);
    auto reduced = limiter.snapshot();
    EXPECT_EQ(reduced.learned_ceiling, 8);
    EXPECT_EQ(reduced.suspect_level, 5);
    EXPECT_EQ(reduced.slow_or_failed_pulls, 1);
    EXPECT_EQ(reduced.reductions, 1);

    for (size_t i = 0; i < 6; ++i)
        limiter.observe(std::chrono::microseconds(100), true,
                        limiter.ownerLimit());
    ASSERT_EQ(limiter.ownerLimit(), 5);

    limiter.observe(std::chrono::milliseconds(200), true, 5);
    EXPECT_EQ(limiter.ownerLimit(), 2);
    reduced = limiter.snapshot();
    EXPECT_EQ(reduced.learned_ceiling, 4);
    EXPECT_EQ(reduced.suspect_level, 0);

    for (size_t i = 0; i < 20; ++i)
        limiter.observe(std::chrono::microseconds(100), true,
                        limiter.ownerLimit());
    auto recovered = limiter.snapshot();
    EXPECT_EQ(recovered.current_owners, 4);
    EXPECT_EQ(recovered.learned_ceiling, 4);
    EXPECT_EQ(recovered.increases, 6);
}

TEST(AdaptiveCreditDispatchLimiter, RpcFailureReducesAtAnyLatency) {
    auto config = controllerConfig(CreditRolloutMode::Required);
    config.adaptive_dispatch_min_owners = 1;
    config.adaptive_dispatch_initial_owners = 2;
    config.adaptive_dispatch_max_owners = 4;
    AdaptiveCreditDispatchLimiter limiter(config);

    limiter.observe(std::chrono::microseconds(1), false, 2);
    EXPECT_EQ(limiter.ownerLimit(), 1);
    EXPECT_EQ(limiter.snapshot().learned_ceiling, 4);
    EXPECT_EQ(limiter.snapshot().suspect_level, 2);
}

TEST(AdaptiveCreditDispatchLimiter, DisabledLeavesStaticWindowUnbounded) {
    auto config = controllerConfig(CreditRolloutMode::Required);
    config.adaptive_dispatch_enabled = false;
    AdaptiveCreditDispatchLimiter limiter(config);

    limiter.observe(std::chrono::seconds(1), false, 2);
    EXPECT_EQ(limiter.ownerLimit(), std::numeric_limits<size_t>::max());
    EXPECT_EQ(limiter.snapshot().slow_or_failed_pulls, 0);
}

}  // namespace
}  // namespace mooncake::tent

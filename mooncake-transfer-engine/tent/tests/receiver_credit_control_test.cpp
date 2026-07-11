// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_control.h"

#include <algorithm>
#include <atomic>
#include <random>
#include <thread>
#include <unordered_set>

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {
CreditKey key() { return {{1, 2}, 3, 4}; }
CreditControlEnvelope envelope(uint64_t sequence, uint64_t total) {
    CreditControlEnvelope e;
    e.key = key();
    e.update.receiver_session_id = e.key.receiver_session;
    e.update.qos_class = e.key.qos_class;
    e.update.epoch = 7;
    e.update.sequence = sequence;
    e.update.grants = {{CreditResource::DataBytes, total}};
    return e;
}

TEST(ReceiverCreditControl, NegotiatesOptionalAndRequiredModes) {
    CreditCapabilityState optional(CreditRolloutMode::Optional);
    ASSERT_TRUE(optional.completeNegotiation({}).ok());
    EXPECT_EQ(optional.state(), CreditPeerState::Legacy);

    CreditCapabilityState required(CreditRolloutMode::Required);
    EXPECT_TRUE(required.completeNegotiation({2}).IsNotImplemented());
    EXPECT_EQ(required.state(), CreditPeerState::Failed);

    ASSERT_TRUE(required.beginNegotiation().ok());
    ASSERT_TRUE(required.completeNegotiation({2, 1}).ok());
    EXPECT_EQ(required.state(), CreditPeerState::Active);
    EXPECT_EQ(required.version(), 1);
    ASSERT_TRUE(required.markStale().ok());
    EXPECT_TRUE(required.refresh(2).IsInvalidArgument());
    ASSERT_TRUE(required.refresh(1).ok());
}

TEST(ReceiverCreditControl, InboxIsBoundedAndDrainIsLimited) {
    BoundedCreditUpdateInbox inbox(2);
    ASSERT_TRUE(inbox.tryPublish(envelope(1, 10)).ok());
    ASSERT_TRUE(inbox.tryPublish(envelope(2, 20)).ok());
    EXPECT_TRUE(inbox.tryPublish(envelope(3, 30)).IsTooManyRequests());
    std::vector<CreditControlEnvelope> drained;
    EXPECT_EQ(inbox.drain(drained, 1), 1);
    EXPECT_EQ(drained[0].update.sequence, 1);
    EXPECT_EQ(inbox.size(), 1);
}

TEST(ReceiverCreditControl, ConcurrentPublishNeverExceedsBound) {
    BoundedCreditUpdateInbox inbox(64);
    std::atomic<int> accepted{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i)
        threads.emplace_back([&, i] {
            for (int j = 0; j < 32; ++j)
                if (inbox.tryPublish(envelope(i * 32 + j + 1, 1)).ok())
                    ++accepted;
        });
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(accepted, 64);
    EXPECT_EQ(inbox.size(), 64);
}

TEST(ReceiverCreditControl, LossDuplicateAndReorderCannotMintCredit) {
    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(key(), 7).ok());
    BoundedCreditUpdateInbox inbox(8);
    // Sequence 2 is lost. Sequence 4 arrives before 3, then 4 is duplicated.
    ASSERT_TRUE(inbox.tryPublish(envelope(1, 10)).ok());
    ASSERT_TRUE(inbox.tryPublish(envelope(4, 40)).ok());
    ASSERT_TRUE(inbox.tryPublish(envelope(3, 30)).ok());
    ASSERT_TRUE(inbox.tryPublish(envelope(4, 400)).ok());
    std::vector<CreditControlEnvelope> drained;
    inbox.drain(drained, 8);
    std::vector<CreditUpdateDisposition> dispositions;
    for (auto& e : drained) {
        CreditUpdateDisposition d;
        ASSERT_TRUE(ledger.applyUpdate(e.key, e.update, d).ok());
        dispositions.push_back(d);
    }
    EXPECT_EQ(dispositions[1], CreditUpdateDisposition::SequenceGap);
    EXPECT_EQ(dispositions[2], CreditUpdateDisposition::DuplicateOrOld);
    EXPECT_EQ(dispositions[3], CreditUpdateDisposition::DuplicateOrOld);
    uint64_t available;
    ASSERT_TRUE(
        ledger.available(key(), CreditResource::DataBytes, available).ok());
    EXPECT_EQ(available, 40);
}

TEST(ReceiverCreditControl, DisconnectMakesActivePeerStale) {
    CreditCapabilityState peer(CreditRolloutMode::Required);
    ASSERT_TRUE(peer.completeNegotiation({1}).ok());
    ASSERT_TRUE(peer.markStale().ok());
    EXPECT_EQ(peer.state(), CreditPeerState::Stale);
    EXPECT_TRUE(peer.completeNegotiation({1}).IsInvalidEntry());
}

TEST(ReceiverCreditControl, ZeroCapacityInboxAlwaysFailsFast) {
    BoundedCreditUpdateInbox inbox(0);
    EXPECT_TRUE(inbox.tryPublish(envelope(1, 1)).IsTooManyRequests());
    std::vector<CreditControlEnvelope> drained;
    EXPECT_EQ(inbox.drain(drained, 100), 0);
}

TEST(ReceiverCreditControl, SustainedConcurrentPublishAndDrainLosesNothing) {
    constexpr int kProducers = 16;
    constexpr int kPerProducer = 2000;
    constexpr int kTotal = kProducers * kPerProducer;
    BoundedCreditUpdateInbox inbox(127);
    std::atomic<int> producers_done{0};
    std::vector<std::thread> producers;
    for (int producer = 0; producer < kProducers; ++producer) {
        producers.emplace_back([&, producer] {
            for (int i = 0; i < kPerProducer; ++i) {
                uint64_t sequence = producer * kPerProducer + i + 1;
                while (!inbox.tryPublish(envelope(sequence, sequence)).ok())
                    std::this_thread::yield();
            }
            ++producers_done;
        });
    }

    std::vector<CreditControlEnvelope> received;
    received.reserve(kTotal);
    while (producers_done != kProducers || inbox.size() != 0) {
        inbox.drain(received, 31);
        std::this_thread::yield();
    }
    for (auto& producer : producers) producer.join();
    EXPECT_EQ(received.size(), static_cast<size_t>(kTotal));
    std::unordered_set<uint64_t> unique;
    for (const auto& item : received) unique.insert(item.update.sequence);
    EXPECT_EQ(unique.size(), static_cast<size_t>(kTotal));
}

TEST(ReceiverCreditControl, DeterministicReorderDuplicateFuzzNeverMints) {
    constexpr uint64_t kUpdates = 10000;
    std::vector<CreditControlEnvelope> traffic;
    traffic.reserve(kUpdates * 2);
    for (uint64_t sequence = 1; sequence <= kUpdates; ++sequence) {
        traffic.push_back(envelope(sequence, sequence * 8));
        if (sequence % 3 == 0)
            traffic.push_back(envelope(sequence, sequence * 8));
    }
    std::mt19937_64 random(0x2849);
    std::shuffle(traffic.begin(), traffic.end(), random);

    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(key(), 7).ok());
    for (const auto& item : traffic) {
        CreditUpdateDisposition disposition;
        ASSERT_TRUE(
            ledger.applyUpdate(item.key, item.update, disposition).ok());
    }
    uint64_t available;
    ASSERT_TRUE(
        ledger.available(key(), CreditResource::DataBytes, available).ok());
    EXPECT_EQ(available, kUpdates * 8);
}

TEST(ReceiverCreditControl, ReconnectRequiresNegotiationAndNewLedgerEpoch) {
    CreditCapabilityState peer(CreditRolloutMode::Required);
    ASSERT_TRUE(peer.completeNegotiation({1}).ok());
    ASSERT_TRUE(peer.markStale().ok());
    ASSERT_TRUE(peer.beginNegotiation().ok());
    ASSERT_TRUE(peer.completeNegotiation({1}).ok());

    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(key(), 7).ok());
    CreditUpdateDisposition disposition;
    ASSERT_TRUE(
        ledger.applyUpdate(key(), envelope(1, 100).update, disposition).ok());
    ASSERT_TRUE(ledger.activate(key(), 8).ok());
    EXPECT_TRUE(ledger.applyUpdate(key(), envelope(2, 200).update, disposition)
                    .IsInvalidEntry());
    auto fresh = envelope(1, 10);
    fresh.update.epoch = 8;
    ASSERT_TRUE(ledger.applyUpdate(key(), fresh.update, disposition).ok());
}
}  // namespace
}  // namespace mooncake::tent

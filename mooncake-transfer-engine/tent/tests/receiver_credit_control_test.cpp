// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_control.h"

#include <atomic>
#include <thread>

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
}  // namespace
}  // namespace mooncake::tent

// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit.h"

#include <atomic>
#include <thread>

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {
CreditKey key() { return {{1, 2}, 3, 4}; }
ReceiverCreditUpdateV1 update(uint64_t epoch, uint64_t seq,
                              std::vector<CreditAmount> grants) {
    ReceiverCreditUpdateV1 u;
    u.receiver_session_id = key().receiver_session;
    u.qos_class = key().qos_class;
    u.epoch = epoch;
    u.sequence = seq;
    u.grants = std::move(grants);
    return u;
}
CreditCharge charge(uint64_t bytes, uint64_t slots) {
    return {{{CreditResource::DataBytes, bytes},
             {CreditResource::RequestSlots, slots}}};
}
void grant(SenderCreditLedger& l, uint64_t seq, uint64_t bytes = 100,
           uint64_t slots = 2) {
    CreditUpdateDisposition d;
    ASSERT_TRUE(l.applyUpdate(key(),
                              update(7, seq,
                                     {{CreditResource::DataBytes, bytes},
                                      {CreditResource::RequestSlots, slots}}),
                              d)
                    .ok());
}

TEST(ReceiverCredit, MultiResourceReserveIsAtomic) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(60, 1)).ok());
    EXPECT_TRUE(l.tryReserve(key(), charge(41, 1)).IsTooManyRequests());
    uint64_t v;
    ASSERT_TRUE(l.available(key(), CreditResource::RequestSlots, v).ok());
    EXPECT_EQ(v, 1);  // failed byte reservation did not consume a slot
}

TEST(ReceiverCredit, DuplicateAndReorderedUpdatesCannotMintCredit) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 2);
    CreditUpdateDisposition d;
    ASSERT_TRUE(l.applyUpdate(
                     key(), update(7, 2, {{CreditResource::DataBytes, 999}}), d)
                    .ok());
    EXPECT_EQ(d, CreditUpdateDisposition::DuplicateOrOld);
    ASSERT_TRUE(l.applyUpdate(
                     key(), update(7, 1, {{CreditResource::DataBytes, 999}}), d)
                    .ok());
    uint64_t v;
    ASSERT_TRUE(l.available(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 100);
}

TEST(ReceiverCredit, SequenceGapIsVisibleAndSafe) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    CreditUpdateDisposition d;
    ASSERT_TRUE(l.applyUpdate(
                     key(), update(7, 4, {{CreditResource::DataBytes, 120}}), d)
                    .ok());
    EXPECT_EQ(d, CreditUpdateDisposition::SequenceGap);
}

TEST(ReceiverCredit, StaleEpochFailsAndActivationFencesOldState) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(60, 1)).ok());
    CreditUpdateDisposition d;
    EXPECT_TRUE(l.applyUpdate(
                     key(), update(6, 2, {{CreditResource::DataBytes, 999}}), d)
                    .IsInvalidEntry());
    ASSERT_TRUE(l.activate(key(), 8).ok());
    uint64_t v;
    EXPECT_TRUE(
        l.available(key(), CreditResource::DataBytes, v).IsInvalidEntry());
}

TEST(ReceiverCredit, ActivationReplayCannotMintCredit) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(80, 1)).ok());
    ASSERT_TRUE(l.activate(key(), 7).ok());  // idempotent, not a reset
    uint64_t v;
    ASSERT_TRUE(l.consumed(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 80);
    EXPECT_TRUE(l.activate(key(), 6).IsInvalidEntry());
    ASSERT_TRUE(l.consumed(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 80);
}

TEST(ReceiverCredit, LedgerEntryCountIsBounded) {
    SenderCreditLedger l(1);
    ASSERT_TRUE(l.activate(key(), 7).ok());
    auto other = key();
    ++other.sender_peer;
    EXPECT_TRUE(l.activate(other, 7).IsTooManyRequests());
    // A new epoch for an existing key does not consume another entry.
    EXPECT_TRUE(l.activate(key(), 8).ok());
}

TEST(ReceiverCredit, DeactivationReleasesCapacityAfterExactEpochFence) {
    SenderCreditLedger l(1);
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(80, 1)).ok());
    auto other = key();
    ++other.sender_peer;
    EXPECT_TRUE(l.activate(other, 1).IsTooManyRequests());

    EXPECT_TRUE(l.deactivate(key(), 6).IsInvalidEntry());
    EXPECT_TRUE(l.activate(other, 1).IsTooManyRequests());
    ASSERT_TRUE(l.deactivate(key(), 7).ok());
    ASSERT_TRUE(l.deactivate(key(), 7).ok());  // cleanup is idempotent
    EXPECT_TRUE(l.activate(other, 1).ok());
}

TEST(ReceiverCredit, OldCleanupCannotEraseReactivatedEpoch) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    ASSERT_TRUE(l.activate(key(), 8).ok());
    EXPECT_TRUE(l.deactivate(key(), 7).IsInvalidEntry());
    CreditUpdateDisposition disposition;
    auto fresh = update(8, 1, {{CreditResource::DataBytes, 10}});
    ASSERT_TRUE(l.applyUpdate(key(), fresh, disposition).ok());
}

TEST(ReceiverCredit, InvalidUpdateDoesNotPartiallyMutate) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    CreditUpdateDisposition d;
    EXPECT_TRUE(l.applyUpdate(key(),
                              update(7, 2,
                                     {{CreditResource::DataBytes, 200},
                                      {CreditResource::RequestSlots, 1}}),
                              d)
                    .IsInvalidArgument());
    uint64_t v;
    ASSERT_TRUE(l.available(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 100);
}

TEST(ReceiverCredit, DuplicateUnknownAndZeroResourcesFailClosed) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    CreditUpdateDisposition d;
    EXPECT_TRUE(l.applyUpdate(key(),
                              update(7, 1,
                                     {{CreditResource::DataBytes, 1},
                                      {CreditResource::DataBytes, 2}}),
                              d)
                    .IsInvalidArgument());
    EXPECT_TRUE(l.tryReserve(key(), {{{static_cast<CreditResource>(99), 1}}})
                    .IsInvalidArgument());
    EXPECT_TRUE(l.tryReserve(key(), {{{CreditResource::DataBytes, 0}}})
                    .IsInvalidArgument());
}

TEST(ReceiverCredit, RollbackChecksUnderflowAtomically) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(60, 1)).ok());
    EXPECT_TRUE(
        l.rollbackReservation(key(), charge(61, 1)).IsInvalidArgument());
    uint64_t v;
    ASSERT_TRUE(l.consumed(key(), CreditResource::RequestSlots, v).ok());
    EXPECT_EQ(v, 1);
    ASSERT_TRUE(l.rollbackReservation(key(), charge(60, 1)).ok());
}

TEST(ReceiverCredit, GrantCannotDecreaseOrFallBelowConsumption) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(80, 1)).ok());
    CreditUpdateDisposition d;
    EXPECT_TRUE(
        l.applyUpdate(key(), update(7, 2, {{CreditResource::DataBytes, 79}}), d)
            .IsInvalidArgument());
}

TEST(ReceiverCredit, ConcurrentReservationsNeverExceedGrant) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1, 100, 100);
    std::atomic<int> admitted{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < 20; ++j)
                if (l.tryReserve(key(), charge(1, 1)).ok()) ++admitted;
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(admitted, 100);
    uint64_t consumed;
    ASSERT_TRUE(l.consumed(key(), CreditResource::DataBytes, consumed).ok());
    EXPECT_EQ(consumed, 100);
}
}  // namespace
}  // namespace mooncake::tent

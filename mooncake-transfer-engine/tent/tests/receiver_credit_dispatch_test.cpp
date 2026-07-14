// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_dispatch.h"

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {

constexpr uint64_t kTarget = 100;
constexpr uint64_t kSender = 200;
constexpr uint32_t kQos = 3;

CreditActivationV1 activation(ReceiverSessionId session, uint64_t epoch) {
    CreditActivationV1 value;
    value.receiver_session_id = session;
    value.epoch = epoch;
    return value;
}

CreditCharge charge(uint64_t bytes, uint64_t request_slots) {
    return {{{CreditResource::DataBytes, bytes},
             {CreditResource::RequestSlots, request_slots}}};
}

void grant(SenderCreditLedger& ledger, const CreditKey& key, uint64_t epoch,
           uint64_t bytes, uint64_t request_slots) {
    ReceiverCreditUpdateV1 update;
    update.receiver_session_id = key.receiver_session;
    update.qos_class = key.qos_class;
    update.epoch = epoch;
    update.sequence = 1;
    update.grants = {{CreditResource::DataBytes, bytes},
                     {CreditResource::RequestSlots, request_slots}};
    CreditUpdateDisposition disposition;
    ASSERT_TRUE(ledger.applyUpdate(key, update, disposition).ok());
}

TEST(ReceiverCreditDispatch, SubmitFailureRollsBackExactMultiResourceCharge) {
    CreditPeerContextTable contexts;
    ASSERT_TRUE(
        contexts.activate(kTarget, kSender, kQos, activation({11, 22}, 7))
            .ok());
    CreditPeerContextSnapshot peer;
    ASSERT_TRUE(contexts.lookup(kTarget, kQos, peer).ok());
    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(peer.key, peer.epoch).ok());
    grant(ledger, peer.key, peer.epoch, 100, 4);
    ReceiverCreditDispatchGate gate(contexts, ledger);
    CreditDispatchSnapshot snapshot;
    ASSERT_TRUE(gate.snapshot(kTarget, kQos, charge(60, 2), snapshot).ok());
    CreditDispatchReservation reservation;
    ASSERT_TRUE(gate.tryReserve(snapshot, reservation).ok());
    ASSERT_TRUE(gate.rollback(reservation).ok());
    EXPECT_EQ(reservation.state, CreditReservationState::RolledBack);
    uint64_t bytes = 0, slots = 0;
    ASSERT_TRUE(
        ledger.available(peer.key, CreditResource::DataBytes, bytes).ok());
    ASSERT_TRUE(
        ledger.available(peer.key, CreditResource::RequestSlots, slots).ok());
    EXPECT_EQ(bytes, 100);
    EXPECT_EQ(slots, 4);
}

TEST(ReceiverCreditDispatch, SubmitSuccessReleasesWithoutMintingCredit) {
    CreditPeerContextTable contexts;
    ASSERT_TRUE(
        contexts.activate(kTarget, kSender, kQos, activation({11, 22}, 7))
            .ok());
    CreditPeerContextSnapshot peer;
    ASSERT_TRUE(contexts.lookup(kTarget, kQos, peer).ok());
    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(peer.key, peer.epoch).ok());
    grant(ledger, peer.key, peer.epoch, 100, 4);
    ReceiverCreditDispatchGate gate(contexts, ledger);
    CreditDispatchSnapshot snapshot;
    ASSERT_TRUE(gate.snapshot(kTarget, kQos, charge(60, 2), snapshot).ok());
    CreditDispatchReservation reservation;
    ASSERT_TRUE(gate.tryReserve(snapshot, reservation).ok());
    ASSERT_TRUE(gate.commit(reservation).ok());
    EXPECT_TRUE(gate.rollback(reservation).IsInvalidEntry());
    ASSERT_TRUE(gate.release(reservation).ok());
    EXPECT_EQ(reservation.state, CreditReservationState::Released);
    EXPECT_TRUE(gate.release(reservation).IsInvalidEntry());
    uint64_t bytes = 0, slots = 0;
    ASSERT_TRUE(
        ledger.available(peer.key, CreditResource::DataBytes, bytes).ok());
    ASSERT_TRUE(
        ledger.available(peer.key, CreditResource::RequestSlots, slots).ok());
    EXPECT_EQ(bytes, 40);
    EXPECT_EQ(slots, 2);
    CreditLedgerSnapshot ledger_snapshot;
    ASSERT_TRUE(ledger.snapshot(peer.key, peer.epoch, ledger_snapshot).ok());
    EXPECT_EQ(ledger_snapshot.consumed[0], 60);
    EXPECT_EQ(ledger_snapshot.consumed[1], 2);
    EXPECT_EQ(ledger_snapshot.completed[0], 60);
    EXPECT_EQ(ledger_snapshot.completed[1], 2);
}

TEST(ReceiverCreditDispatch, OldEpochRollbackAndReleaseLeaveNewEpochUntouched) {
    CreditPeerContextTable contexts;
    ASSERT_TRUE(
        contexts.activate(kTarget, kSender, kQos, activation({11, 22}, 7))
            .ok());
    CreditPeerContextSnapshot peer;
    ASSERT_TRUE(contexts.lookup(kTarget, kQos, peer).ok());
    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(peer.key, peer.epoch).ok());
    grant(ledger, peer.key, peer.epoch, 100, 4);
    ReceiverCreditDispatchGate gate(contexts, ledger);
    CreditDispatchSnapshot snapshot;
    ASSERT_TRUE(gate.snapshot(kTarget, kQos, charge(20, 1), snapshot).ok());
    CreditDispatchReservation pending;
    CreditDispatchReservation committed;
    ASSERT_TRUE(gate.tryReserve(snapshot, pending).ok());
    ASSERT_TRUE(gate.tryReserve(snapshot, committed).ok());
    ASSERT_TRUE(gate.commit(committed).ok());

    ASSERT_TRUE(ledger.activate(peer.key, 8).ok());
    grant(ledger, peer.key, 8, 50, 2);
    ASSERT_TRUE(ledger.tryReserve(peer.key, charge(10, 1)).ok());

    EXPECT_TRUE(gate.rollback(pending).IsInvalidEntry());
    EXPECT_TRUE(gate.release(committed).IsInvalidEntry());
    EXPECT_EQ(pending.state, CreditReservationState::Reserved);
    EXPECT_EQ(committed.state, CreditReservationState::Committed);
    CreditLedgerSnapshot current;
    ASSERT_TRUE(ledger.snapshot(peer.key, 8, current).ok());
    EXPECT_EQ(current.consumed[0], 10);
    EXPECT_EQ(current.consumed[1], 1);
    EXPECT_EQ(current.completed[0], 0);
    EXPECT_EQ(current.completed[1], 0);
}

TEST(ReceiverCreditDispatch, ReceiverRestartRejectsQueuedOldSnapshot) {
    CreditPeerContextTable contexts;
    ASSERT_TRUE(
        contexts.activate(kTarget, kSender, kQos, activation({11, 22}, 7))
            .ok());
    CreditPeerContextSnapshot old_peer;
    ASSERT_TRUE(contexts.lookup(kTarget, kQos, old_peer).ok());
    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(old_peer.key, old_peer.epoch).ok());
    grant(ledger, old_peer.key, old_peer.epoch, 100, 4);
    ReceiverCreditDispatchGate gate(contexts, ledger);
    CreditDispatchSnapshot old_snapshot;
    ASSERT_TRUE(gate.snapshot(kTarget, kQos, charge(60, 2), old_snapshot).ok());

    ASSERT_TRUE(
        contexts.activate(kTarget, kSender, kQos, activation({33, 44}, 1))
            .ok());
    CreditDispatchReservation reservation;
    EXPECT_TRUE(gate.tryReserve(old_snapshot, reservation).IsInvalidEntry());
    uint64_t available = 0;
    ASSERT_TRUE(
        ledger.available(old_peer.key, CreditResource::DataBytes, available)
            .ok());
    EXPECT_EQ(available, 100);
}

TEST(ReceiverCreditDispatch, ReserveFailureLeavesTokenAndLedgerUntouched) {
    CreditPeerContextTable contexts;
    ASSERT_TRUE(
        contexts.activate(kTarget, kSender, kQos, activation({11, 22}, 7))
            .ok());
    CreditPeerContextSnapshot peer;
    ASSERT_TRUE(contexts.lookup(kTarget, kQos, peer).ok());
    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(peer.key, peer.epoch).ok());
    grant(ledger, peer.key, peer.epoch, 50, 1);
    ReceiverCreditDispatchGate gate(contexts, ledger);
    CreditDispatchSnapshot snapshot;
    ASSERT_TRUE(gate.snapshot(kTarget, kQos, charge(60, 2), snapshot).ok());
    CreditDispatchReservation reservation;
    EXPECT_TRUE(gate.tryReserve(snapshot, reservation).IsTooManyRequests());
    EXPECT_EQ(reservation.state, CreditReservationState::Empty);
    uint64_t available = 0;
    ASSERT_TRUE(
        ledger.available(peer.key, CreditResource::DataBytes, available).ok());
    EXPECT_EQ(available, 50);
}

}  // namespace
}  // namespace mooncake::tent

// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_dispatch.h"

namespace mooncake::tent {

Status ReceiverCreditDispatchGate::snapshot(
    uint64_t target_id, uint32_t qos_class, CreditCharge charge,
    CreditDispatchSnapshot& output) const {
    if (charge.resources.empty())
        return Status::InvalidArgument(
            "invalid receiver credit dispatch snapshot" LOC_MARK);
    CreditPeerContextSnapshot peer;
    CHECK_STATUS(contexts_.lookup(target_id, qos_class, peer));
    CreditDispatchSnapshot next;
    next.target_id = target_id;
    next.qos_class = qos_class;
    next.key = peer.key;
    next.epoch = peer.epoch;
    next.charge = std::move(charge);
    output = std::move(next);
    return Status::OK();
}

Status ReceiverCreditDispatchGate::tryReserve(
    const CreditDispatchSnapshot& snapshot,
    CreditDispatchReservation& reservation) {
    if (reservation.state != CreditReservationState::Empty)
        return Status::InvalidEntry(
            "receiver credit reservation token already used" LOC_MARK);
    CreditPeerContextSnapshot current;
    CHECK_STATUS(
        contexts_.lookup(snapshot.target_id, snapshot.qos_class, current));
    if (!(current.key == snapshot.key) || current.epoch != snapshot.epoch)
        return Status::InvalidEntry(
            "receiver credit dispatch snapshot is stale" LOC_MARK);
    CHECK_STATUS(ledger_.tryReserve(snapshot.key, snapshot.charge));
    reservation.snapshot = snapshot;
    reservation.state = CreditReservationState::Reserved;
    return Status::OK();
}

Status ReceiverCreditDispatchGate::commit(
    CreditDispatchReservation& reservation) {
    if (reservation.state != CreditReservationState::Reserved)
        return Status::InvalidEntry(
            "receiver credit reservation is not pending" LOC_MARK);
    reservation.state = CreditReservationState::Committed;
    return Status::OK();
}

Status ReceiverCreditDispatchGate::rollback(
    CreditDispatchReservation& reservation) {
    if (reservation.state != CreditReservationState::Reserved)
        return Status::InvalidEntry(
            "receiver credit reservation is not pending" LOC_MARK);
    CHECK_STATUS(ledger_.rollbackReservation(reservation.snapshot.key,
                                             reservation.snapshot.charge));
    reservation.state = CreditReservationState::RolledBack;
    return Status::OK();
}

}  // namespace mooncake::tent

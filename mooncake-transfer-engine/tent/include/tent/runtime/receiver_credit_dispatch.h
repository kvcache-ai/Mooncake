// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_RUNTIME_RECEIVER_CREDIT_DISPATCH_H
#define TENT_RUNTIME_RECEIVER_CREDIT_DISPATCH_H

#include <cstdint>

#include "tent/runtime/receiver_credit.h"
#include "tent/runtime/receiver_credit_control.h"

namespace mooncake::tent {

struct CreditDispatchSnapshot {
    uint64_t target_id{0};
    uint32_t qos_class{0};
    CreditKey key;
    uint64_t epoch{0};
    CreditCharge charge;
};

enum class CreditReservationState : uint8_t {
    Empty,
    Reserved,
    Committed,
    RolledBack
};

struct CreditDispatchReservation {
    CreditDispatchSnapshot snapshot;
    CreditReservationState state{CreditReservationState::Empty};
};

// Runtime-owner helper for the dispatch boundary. The owner must serialize
// peer activation/update processing with reserve/commit/rollback calls.
class ReceiverCreditDispatchGate {
   public:
    ReceiverCreditDispatchGate(const CreditPeerContextTable& contexts,
                               SenderCreditLedger& ledger)
        : contexts_(contexts), ledger_(ledger) {}

    Status snapshot(uint64_t target_id, uint32_t qos_class, CreditCharge charge,
                    CreditDispatchSnapshot& output) const;
    Status tryReserve(const CreditDispatchSnapshot& snapshot,
                      CreditDispatchReservation& reservation);
    Status commit(CreditDispatchReservation& reservation);
    Status rollback(CreditDispatchReservation& reservation);

   private:
    const CreditPeerContextTable& contexts_;
    SenderCreditLedger& ledger_;
};

}  // namespace mooncake::tent

#endif

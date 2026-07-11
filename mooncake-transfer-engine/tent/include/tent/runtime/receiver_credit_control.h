// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_RUNTIME_RECEIVER_CREDIT_CONTROL_H
#define TENT_RUNTIME_RECEIVER_CREDIT_CONTROL_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "tent/runtime/receiver_credit.h"

namespace mooncake::tent {

enum class CreditRolloutMode : uint8_t { Disabled, Optional, Required };
enum class CreditPeerState : uint8_t {
    Disabled,
    Negotiating,
    Legacy,
    Active,
    Stale,
    Failed
};

class CreditCapabilityState {
   public:
    explicit CreditCapabilityState(CreditRolloutMode mode);
    Status beginNegotiation();
    Status completeNegotiation(const std::vector<uint16_t>& peer_versions);
    Status markStale();
    Status refresh(uint16_t version);
    CreditPeerState state() const { return state_; }
    uint16_t version() const { return version_; }

   private:
    CreditRolloutMode mode_;
    CreditPeerState state_;
    uint16_t version_{0};
};

struct CreditControlEnvelope {
    CreditKey key;
    ReceiverCreditUpdateV1 update;
};

// Bounded, nonblocking publisher queue. Control callbacks only validate and
// enqueue; the runtime owner drains and mutates the ledger.
class BoundedCreditUpdateInbox {
   public:
    explicit BoundedCreditUpdateInbox(size_t capacity) : capacity_(capacity) {}
    Status tryPublish(CreditControlEnvelope envelope);
    size_t drain(std::vector<CreditControlEnvelope>& output,
                 size_t max_updates);
    size_t size() const;

   private:
    const size_t capacity_;
    mutable std::mutex mutex_;
    std::deque<CreditControlEnvelope> queue_;
};

class ReceiverCreditCodecV1 {
   public:
    static constexpr size_t kHeaderBytes = 52;
    static constexpr size_t kGrantBytes = 12;
    static constexpr size_t kMaxWireBytes =
        kHeaderBytes + kCreditResourceCount * kGrantBytes;

    static Status encode(const ReceiverCreditUpdateV1& update,
                         std::string& wire);
    static Status decode(std::string_view wire, ReceiverCreditUpdateV1& update);
};

}  // namespace mooncake::tent
#endif

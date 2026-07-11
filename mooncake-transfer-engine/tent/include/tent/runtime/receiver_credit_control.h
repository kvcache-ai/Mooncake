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

class CreditCapabilityCodecV1 {
   public:
    static constexpr size_t kHeaderBytes = 8;
    static constexpr size_t kMaxVersions = 8;
    static constexpr size_t kMaxWireBytes =
        kHeaderBytes + kMaxVersions * sizeof(uint16_t);

    static Status encode(const std::vector<uint16_t>& versions,
                         std::string& wire);
    static Status decode(std::string_view wire,
                         std::vector<uint16_t>& versions);
};

struct CreditActivationV1 {
    uint16_t schema_version{1};
    uint16_t chosen_version{1};
    ReceiverSessionId receiver_session_id;
    uint64_t epoch{0};
    uint32_t freshness_ttl_ms{0};
};

class CreditActivationCodecV1 {
   public:
    static constexpr size_t kWireBytes = 40;
    static Status encode(const CreditActivationV1& activation,
                         std::string& wire);
    static Status decode(std::string_view wire,
                         CreditActivationV1& activation);
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

// Transport-neutral ingress used by a control-plane callback. It performs no
// ledger mutation and never blocks waiting for queue capacity.
class ReceiverCreditIngress {
   public:
    ReceiverCreditIngress(BoundedCreditUpdateInbox& inbox, CreditKey key,
                          uint64_t epoch)
        : inbox_(inbox), key_(key), epoch_(epoch) {}

    Status tryAccept(std::string_view wire);

   private:
    BoundedCreditUpdateInbox& inbox_;
    CreditKey key_;
    uint64_t epoch_;
};

}  // namespace mooncake::tent
#endif

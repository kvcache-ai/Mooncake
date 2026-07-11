// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_RUNTIME_RECEIVER_CREDIT_H
#define TENT_RUNTIME_RECEIVER_CREDIT_H

#include <array>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tent/common/status.h"

namespace mooncake::tent {

struct ReceiverSessionId {
    uint64_t high{0}, low{0};
    bool operator==(const ReceiverSessionId& o) const {
        return high == o.high && low == o.low;
    }
};
struct CreditKey {
    ReceiverSessionId receiver_session;
    uint64_t sender_peer{0};
    uint32_t qos_class{0};
    bool operator==(const CreditKey& o) const {
        return receiver_session == o.receiver_session &&
               sender_peer == o.sender_peer && qos_class == o.qos_class;
    }
};
struct CreditKeyHash {
    size_t operator()(const CreditKey&) const noexcept;
};
enum class CreditResource : uint16_t {
    DataBytes = 1,
    RequestSlots,
    StagingSlots,
    ConsumerSlots
};
constexpr size_t kCreditResourceCount = 4;
struct CreditAmount {
    CreditResource resource;
    uint64_t grant_total{0};
};
struct CreditCharge {
    std::vector<std::pair<CreditResource, uint64_t>> resources;
};
struct ReceiverCreditUpdateV1 {
    uint16_t schema_version{1}, flags{0};
    uint32_t qos_class{0};
    ReceiverSessionId receiver_session_id;
    uint64_t epoch{0}, sequence{0};
    uint32_t freshness_ttl_ms{0};
    std::vector<CreditAmount> grants;
};
enum class CreditUpdateDisposition : uint8_t {
    Applied,
    DuplicateOrOld,
    SequenceGap
};

// Private, sender-side state model. It has no network or Admission integration.
class SenderCreditLedger {
   public:
    explicit SenderCreditLedger(size_t max_entries = 1024)
        : max_entries_(max_entries) {}
    Status activate(const CreditKey&, uint64_t epoch);
    Status applyUpdate(const CreditKey&, const ReceiverCreditUpdateV1&,
                       CreditUpdateDisposition&);
    Status tryReserve(const CreditKey&, const CreditCharge&);
    // Only for work not yet handed to a transport; completions need a new
    // grant.
    Status rollbackReservation(const CreditKey&, const CreditCharge&);
    Status available(const CreditKey&, CreditResource, uint64_t&) const;
    Status consumed(const CreditKey&, CreditResource, uint64_t&) const;

   private:
    struct Entry {
        uint64_t epoch{0}, last_sequence{0};
        bool has_update{false};
        std::array<uint64_t, kCreditResourceCount> grants{}, consumed{};
    };
    static Status resourceIndex(CreditResource, size_t&);
    static Status normalize(const CreditCharge&,
                            std::array<uint64_t, kCreditResourceCount>&);
    mutable std::mutex mutex_;
    const size_t max_entries_;
    std::unordered_map<CreditKey, Entry, CreditKeyHash> entries_;
};

}  // namespace mooncake::tent
#endif

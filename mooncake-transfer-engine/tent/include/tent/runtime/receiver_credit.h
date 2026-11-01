// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_RUNTIME_RECEIVER_CREDIT_H
#define TENT_RUNTIME_RECEIVER_CREDIT_H

#include <array>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tent/common/status.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake::tent {

// A receiver session changes whenever a receiver process restarts.  A sender
// instance has the same representation, but remains a distinct type so the
// two identities cannot be accidentally exchanged at an API boundary.
struct ReceiverSessionId {
    uint64_t high{0}, low{0};
    bool operator==(const ReceiverSessionId&) const = default;
    [[nodiscard]] bool empty() const noexcept { return high == 0 && low == 0; }
};

struct SenderInstanceId {
    uint64_t high{0}, low{0};
    bool operator==(const SenderInstanceId&) const = default;
    [[nodiscard]] bool empty() const noexcept { return high == 0 && low == 0; }
};

struct CreditKey {
    ReceiverSessionId receiver_session;
    SenderInstanceId sender_instance;
    uint32_t qos_class{0};
    bool operator==(const CreditKey&) const = default;
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
constexpr uint16_t kReceiverCreditProtocolVersion = 1;
constexpr uint16_t kReceiverCreditRequired = 1U << 0;

// A cumulative grant.  grant_total never decreases within a session/epoch.
struct CreditAmount {
    CreditResource resource{CreditResource::DataBytes};
    uint64_t grant_total{0};
};

// A cumulative counter used for release reports and desired grant totals.
struct CreditCounter {
    CreditResource resource{CreditResource::DataBytes};
    uint64_t total{0};
};

struct CreditCharge {
    std::vector<std::pair<CreditResource, uint64_t>> resources;
};

struct ReceiverCreditAdvertV1 {
    uint16_t schema_version{kReceiverCreditProtocolVersion};
    uint16_t flags{kReceiverCreditRequired};
    ReceiverSessionId receiver_session_id;
    uint64_t epoch{0};
    uint32_t freshness_ttl_ms{0};
    std::vector<CreditResource> resources;
    // Explicit capacities let a sender reject a request that can never fit
    // instead of leaving it indefinitely deferred behind the credit gate.
    std::vector<CreditCounter> capacities;
};

struct ReceiverCreditUpdateV1 {
    uint16_t schema_version{kReceiverCreditProtocolVersion}, flags{0};
    uint32_t qos_class{0};
    ReceiverSessionId receiver_session_id;
    uint64_t epoch{0}, sequence{0};
    uint32_t freshness_ttl_ms{0};
    std::vector<CreditAmount> grants;
};

// One cumulative exchange is sufficient for initial credit, replenishment,
// replay recovery, and terminal release reporting.  desired_grant_totals is
// absolute (not a delta), making retries idempotent.
struct ReceiverCreditExchangeRequestV1 {
    uint16_t schema_version{kReceiverCreditProtocolVersion}, flags{0};
    ReceiverSessionId expected_receiver_session_id;
    uint64_t epoch{0};
    SenderInstanceId sender_instance_id;
    uint32_t qos_class{0};
    uint64_t known_update_sequence{0};
    uint64_t report_sequence{0};
    std::vector<CreditCounter> released_totals;
    std::vector<CreditCounter> desired_grant_totals;
};

struct ReceiverCreditExchangeReplyV1 {
    uint16_t schema_version{kReceiverCreditProtocolVersion}, flags{0};
    SenderInstanceId sender_instance_id;
    ReceiverCreditUpdateV1 update;
    std::string reply_msg;
};

enum class CreditUpdateDisposition : uint8_t {
    Applied,
    DuplicateOrOld,
    SequenceGap
};

using CreditNowProvider = std::function<uint64_t()>;

// Sender-side cumulative ledger.  A successful transport handoff consumes a
// grant permanently; terminal completion reports a cumulative release to the
// receiver, which is the only authority allowed to mint a replacement grant.
class SenderCreditLedger {
   public:
    explicit SenderCreditLedger(size_t max_entries = 1024,
                                uint32_t max_freshness_ttl_ms = 60000,
                                CreditNowProvider now_provider = {});

    Status activate(const CreditKey&, uint64_t epoch);
    Status deactivate(const CreditKey&, uint64_t epoch);
    Status deactivateSession(const ReceiverSessionId&);
    Status applyUpdate(const CreditKey&, const ReceiverCreditUpdateV1&,
                       CreditUpdateDisposition&);

    Status tryReserve(const CreditKey&, const CreditCharge&);
    // Only for a failed submit that is proven to have handed no work to a
    // transport. Consumption remains cumulative and the same amount is added
    // to released, so the receiver (rather than this sender) reclaims it.
    Status rollbackReservation(const CreditKey&, const CreditCharge&);
    // Records terminal completion of committed work.  It deliberately does
    // not reduce consumed; the receiver must authorize replacement credit.
    Status releaseCommitted(const CreditKey&, const CreditCharge&);

    // Builds the next idempotent exchange.  The desired totals request enough
    // credit for `next_charge` in addition to all locally consumed credit.
    Status prepareExchange(const CreditKey&, const CreditCharge& next_charge,
                           ReceiverCreditExchangeRequestV1&);
    // Reports terminal releases without asking the receiver to replenish an
    // idle sender.  A later prepareExchange() requests credit on demand.
    Status prepareReleaseExchange(const CreditKey&,
                                  ReceiverCreditExchangeRequestV1&);

    Status available(const CreditKey&, CreditResource, uint64_t&) const;
    Status consumed(const CreditKey&, CreditResource, uint64_t&) const;
    Status released(const CreditKey&, CreditResource, uint64_t&) const;

   private:
    struct Entry {
        uint64_t epoch{0}, last_sequence{0}, report_sequence{0};
        uint64_t expires_at_ns{0};
        bool has_update{false};
        std::array<uint64_t, kCreditResourceCount> grants{}, consumed{},
            released{};
    };

    static Status resourceIndex(CreditResource, size_t&);
    static Status normalize(const CreditCharge&,
                            std::array<uint64_t, kCreditResourceCount>&);
    uint64_t nowNanos() const;
    Status requireFresh(const Entry&) const;

    mutable std::mutex mutex_;
    const size_t max_entries_;
    const uint32_t max_freshness_ttl_ms_;
    CreditNowProvider now_provider_;
    std::unordered_map<CreditKey, Entry, CreditKeyHash> entries_;
};

struct ReceiverCreditLimits {
    std::array<uint64_t, kCreditResourceCount> capacity{};
    uint32_t freshness_ttl_ms{1000};
    size_t max_senders{1024};
    // Replay history is retained for the receiver session so an old initial
    // demand cannot be accepted again after all of its grants are released.
    // The bound is fail-closed; entries are reset only by a new receiver
    // session, which also fences every old request on the wire.
    size_t max_entries{4096};
};

// Receiver-side grant authority.  All resources in one desired request are
// granted atomically, and aggregate outstanding authorization can never exceed
// the configured capacity.  Expiry is sender-side freshness only: this class
// conservatively does not recycle an unreachable sender without a transport
// fence proving its old DMA is gone.
class ReceiverCreditAuthority {
   public:
    ReceiverCreditAuthority(ReceiverSessionId session, uint64_t epoch,
                            ReceiverCreditLimits limits);

    Status status() const { return status_; }
    ReceiverCreditAdvertV1 advert() const;
    Status exchange(const ReceiverCreditExchangeRequestV1&,
                    ReceiverCreditExchangeReplyV1&);
    Status outstanding(CreditResource, uint64_t&) const;

   private:
    struct Entry {
        uint64_t last_report_sequence{0};
        uint64_t update_sequence{0};
        std::array<uint64_t, kCreditResourceCount> grants{}, released{};
    };

    static Status validateLimits(const ReceiverCreditLimits&);
    static Status normalizeCounters(const std::vector<CreditCounter>&,
                                    std::array<uint64_t, kCreditResourceCount>&,
                                    std::array<bool, kCreditResourceCount>&);
    void fillReply(const CreditKey&, const Entry&,
                   ReceiverCreditExchangeReplyV1&) const;

    ReceiverSessionId session_;
    uint64_t epoch_{0};
    ReceiverCreditLimits limits_;
    Status status_;
    mutable std::mutex mutex_;
    std::array<uint64_t, kCreditResourceCount> outstanding_{};
    std::unordered_map<CreditKey, Entry, CreditKeyHash> entries_;
};

inline void to_json(nlohmann::json& j, const ReceiverSessionId& id) {
    j = nlohmann::json{{"high", id.high}, {"low", id.low}};
}
inline void from_json(const nlohmann::json& j, ReceiverSessionId& id) {
    j.at("high").get_to(id.high);
    j.at("low").get_to(id.low);
}
inline void to_json(nlohmann::json& j, const SenderInstanceId& id) {
    j = nlohmann::json{{"high", id.high}, {"low", id.low}};
}
inline void from_json(const nlohmann::json& j, SenderInstanceId& id) {
    j.at("high").get_to(id.high);
    j.at("low").get_to(id.low);
}
inline void to_json(nlohmann::json& j, const CreditResource& resource) {
    j = static_cast<uint16_t>(resource);
}
inline void from_json(const nlohmann::json& j, CreditResource& resource) {
    resource = static_cast<CreditResource>(j.get<uint16_t>());
}
inline void to_json(nlohmann::json& j, const CreditAmount& amount) {
    j = nlohmann::json{{"resource", amount.resource},
                       {"grant_total", amount.grant_total}};
}
inline void from_json(const nlohmann::json& j, CreditAmount& amount) {
    j.at("resource").get_to(amount.resource);
    j.at("grant_total").get_to(amount.grant_total);
}
inline void to_json(nlohmann::json& j, const CreditCounter& counter) {
    j = nlohmann::json{{"resource", counter.resource},
                       {"total", counter.total}};
}
inline void from_json(const nlohmann::json& j, CreditCounter& counter) {
    j.at("resource").get_to(counter.resource);
    j.at("total").get_to(counter.total);
}
inline void to_json(nlohmann::json& j, const ReceiverCreditAdvertV1& advert) {
    j = nlohmann::json{{"schema_version", advert.schema_version},
                       {"flags", advert.flags},
                       {"receiver_session_id", advert.receiver_session_id},
                       {"epoch", advert.epoch},
                       {"freshness_ttl_ms", advert.freshness_ttl_ms},
                       {"resources", advert.resources},
                       {"capacities", advert.capacities}};
}
inline void from_json(const nlohmann::json& j, ReceiverCreditAdvertV1& advert) {
    advert.schema_version = j.value("schema_version", uint16_t{0});
    advert.flags = j.value("flags", uint16_t{0});
    j.at("receiver_session_id").get_to(advert.receiver_session_id);
    advert.epoch = j.value("epoch", uint64_t{0});
    advert.freshness_ttl_ms = j.value("freshness_ttl_ms", uint32_t{0});
    advert.resources = j.value("resources", std::vector<CreditResource>{});
    advert.capacities = j.value("capacities", std::vector<CreditCounter>{});
}
inline void to_json(nlohmann::json& j, const ReceiverCreditUpdateV1& update) {
    j = nlohmann::json{{"schema_version", update.schema_version},
                       {"flags", update.flags},
                       {"qos_class", update.qos_class},
                       {"receiver_session_id", update.receiver_session_id},
                       {"epoch", update.epoch},
                       {"sequence", update.sequence},
                       {"freshness_ttl_ms", update.freshness_ttl_ms},
                       {"grants", update.grants}};
}
inline void from_json(const nlohmann::json& j, ReceiverCreditUpdateV1& update) {
    update.schema_version = j.value("schema_version", uint16_t{0});
    update.flags = j.value("flags", uint16_t{0});
    update.qos_class = j.value("qos_class", uint32_t{0});
    j.at("receiver_session_id").get_to(update.receiver_session_id);
    update.epoch = j.value("epoch", uint64_t{0});
    update.sequence = j.value("sequence", uint64_t{0});
    update.freshness_ttl_ms = j.value("freshness_ttl_ms", uint32_t{0});
    update.grants = j.value("grants", std::vector<CreditAmount>{});
}
inline void to_json(nlohmann::json& j,
                    const ReceiverCreditExchangeRequestV1& request) {
    j = nlohmann::json{
        {"schema_version", request.schema_version},
        {"flags", request.flags},
        {"expected_receiver_session_id", request.expected_receiver_session_id},
        {"epoch", request.epoch},
        {"sender_instance_id", request.sender_instance_id},
        {"qos_class", request.qos_class},
        {"known_update_sequence", request.known_update_sequence},
        {"report_sequence", request.report_sequence},
        {"released_totals", request.released_totals},
        {"desired_grant_totals", request.desired_grant_totals}};
}
inline void from_json(const nlohmann::json& j,
                      ReceiverCreditExchangeRequestV1& request) {
    request.schema_version = j.value("schema_version", uint16_t{0});
    request.flags = j.value("flags", uint16_t{0});
    j.at("expected_receiver_session_id")
        .get_to(request.expected_receiver_session_id);
    request.epoch = j.value("epoch", uint64_t{0});
    j.at("sender_instance_id").get_to(request.sender_instance_id);
    request.qos_class = j.value("qos_class", uint32_t{0});
    request.known_update_sequence =
        j.value("known_update_sequence", uint64_t{0});
    request.report_sequence = j.value("report_sequence", uint64_t{0});
    request.released_totals =
        j.value("released_totals", std::vector<CreditCounter>{});
    request.desired_grant_totals =
        j.value("desired_grant_totals", std::vector<CreditCounter>{});
}
inline void to_json(nlohmann::json& j,
                    const ReceiverCreditExchangeReplyV1& reply) {
    j = nlohmann::json{{"schema_version", reply.schema_version},
                       {"flags", reply.flags},
                       {"sender_instance_id", reply.sender_instance_id},
                       {"update", reply.update},
                       {"reply_msg", reply.reply_msg}};
}
inline void from_json(const nlohmann::json& j,
                      ReceiverCreditExchangeReplyV1& reply) {
    reply.schema_version = j.value("schema_version", uint16_t{0});
    reply.flags = j.value("flags", uint16_t{0});
    j.at("sender_instance_id").get_to(reply.sender_instance_id);
    j.at("update").get_to(reply.update);
    reply.reply_msg = j.value("reply_msg", "");
}

}  // namespace mooncake::tent
#endif

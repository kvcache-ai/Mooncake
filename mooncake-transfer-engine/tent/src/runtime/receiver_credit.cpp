// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <unordered_set>

namespace mooncake::tent {
namespace {

uint64_t steadyNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

Status indexFor(CreditResource resource, size_t& index) {
    const auto raw = static_cast<uint16_t>(resource);
    if (raw < 1 || raw > kCreditResourceCount) {
        return Status::InvalidArgument("unknown credit resource" LOC_MARK);
    }
    index = raw - 1;
    return Status::OK();
}

void mixHash(size_t& hash, uint64_t value) {
    hash ^= std::hash<uint64_t>{}(value) + 0x9e3779b97f4a7c15ULL + (hash << 6) +
            (hash >> 2);
}

struct SenderInstanceIdHash {
    size_t operator()(const SenderInstanceId& id) const noexcept {
        size_t hash = std::hash<uint64_t>{}(id.high);
        mixHash(hash, id.low);
        return hash;
    }
};

}  // namespace

size_t CreditKeyHash::operator()(const CreditKey& key) const noexcept {
    size_t hash = std::hash<uint64_t>{}(key.receiver_session.high);
    mixHash(hash, key.receiver_session.low);
    mixHash(hash, key.sender_instance.high);
    mixHash(hash, key.sender_instance.low);
    mixHash(hash, key.qos_class);
    return hash;
}

SenderCreditLedger::SenderCreditLedger(size_t max_entries,
                                       uint32_t max_freshness_ttl_ms,
                                       CreditNowProvider now_provider)
    : max_entries_(max_entries),
      max_freshness_ttl_ms_(max_freshness_ttl_ms),
      now_provider_(std::move(now_provider)) {}

Status SenderCreditLedger::resourceIndex(CreditResource resource,
                                         size_t& index) {
    return indexFor(resource, index);
}

Status SenderCreditLedger::normalize(
    const CreditCharge& charge,
    std::array<uint64_t, kCreditResourceCount>& normalized) {
    normalized.fill(0);
    if (charge.resources.empty()) {
        return Status::InvalidArgument("empty credit charge" LOC_MARK);
    }
    for (const auto& [resource, amount] : charge.resources) {
        size_t index = 0;
        CHECK_STATUS(resourceIndex(resource, index));
        if (amount == 0 || normalized[index] != 0) {
            return Status::InvalidArgument(
                "zero or duplicate credit charge" LOC_MARK);
        }
        normalized[index] = amount;
    }
    return Status::OK();
}

uint64_t SenderCreditLedger::nowNanos() const {
    return now_provider_ ? now_provider_() : steadyNowNs();
}

Status SenderCreditLedger::requireFresh(const Entry& entry) const {
    if (!entry.has_update || entry.expires_at_ns == 0 ||
        nowNanos() >= entry.expires_at_ns) {
        return Status::InvalidEntry("receiver credit is stale" LOC_MARK);
    }
    return Status::OK();
}

Status SenderCreditLedger::activate(const CreditKey& key, uint64_t epoch) {
    if (key.receiver_session.empty() || key.sender_instance.empty() ||
        epoch == 0) {
        return Status::InvalidArgument(
            "invalid credit activation identity" LOC_MARK);
    }
    std::lock_guard lock(mutex_);
    auto existing = entries_.find(key);
    if (existing != entries_.end()) {
        if (epoch < existing->second.epoch) {
            return Status::InvalidEntry("stale credit activation" LOC_MARK);
        }
        if (epoch == existing->second.epoch) return Status::OK();
        Entry replacement;
        replacement.epoch = epoch;
        existing->second = replacement;
        return Status::OK();
    }
    if (entries_.size() >= max_entries_) {
        return Status::TooManyRequests("credit ledger entry limit" LOC_MARK);
    }
    Entry entry;
    entry.epoch = epoch;
    entries_.emplace(key, entry);
    return Status::OK();
}

Status SenderCreditLedger::deactivate(const CreditKey& key, uint64_t epoch) {
    if (epoch == 0) {
        return Status::InvalidArgument("zero credit epoch" LOC_MARK);
    }
    std::lock_guard lock(mutex_);
    auto existing = entries_.find(key);
    if (existing == entries_.end()) return Status::OK();
    if (existing->second.epoch != epoch) {
        return Status::InvalidEntry("credit cleanup epoch mismatch" LOC_MARK);
    }
    entries_.erase(existing);
    return Status::OK();
}

Status SenderCreditLedger::deactivateSession(const ReceiverSessionId& session) {
    if (session.empty()) {
        return Status::InvalidArgument(
            "empty receiver credit session cleanup" LOC_MARK);
    }
    std::lock_guard lock(mutex_);
    for (auto it = entries_.begin(); it != entries_.end();) {
        if (it->first.receiver_session == session) {
            it = entries_.erase(it);
        } else {
            ++it;
        }
    }
    return Status::OK();
}

Status SenderCreditLedger::applyUpdate(const CreditKey& key,
                                       const ReceiverCreditUpdateV1& update,
                                       CreditUpdateDisposition& disposition) {
    if (update.schema_version != kReceiverCreditProtocolVersion ||
        update.flags != 0 || update.epoch == 0 || update.sequence == 0 ||
        update.freshness_ttl_ms == 0 ||
        update.freshness_ttl_ms > max_freshness_ttl_ms_) {
        return Status::InvalidArgument("invalid credit update header" LOC_MARK);
    }
    if (!(update.receiver_session_id == key.receiver_session) ||
        update.qos_class != key.qos_class ||
        update.grants.size() > kCreditResourceCount) {
        return Status::InvalidArgument("credit update identity/size" LOC_MARK);
    }

    std::array<uint64_t, kCreditResourceCount> proposed{};
    std::array<bool, kCreditResourceCount> present{};
    for (const auto& amount : update.grants) {
        size_t index = 0;
        CHECK_STATUS(resourceIndex(amount.resource, index));
        if (present[index]) {
            return Status::InvalidArgument("duplicate grant resource" LOC_MARK);
        }
        present[index] = true;
        proposed[index] = amount.grant_total;
    }

    std::lock_guard lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end() || it->second.epoch != update.epoch) {
        return Status::InvalidEntry("inactive or stale credit epoch" LOC_MARK);
    }
    auto& entry = it->second;
    if (entry.has_update && update.sequence <= entry.last_sequence) {
        disposition = CreditUpdateDisposition::DuplicateOrOld;
        return Status::OK();
    }
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (present[i] && (proposed[i] < entry.grants[i] ||
                           proposed[i] < entry.consumed[i])) {
            return Status::InvalidArgument(
                "decreasing or under-consumed grant" LOC_MARK);
        }
    }

    const bool gap =
        entry.has_update && update.sequence > entry.last_sequence + 1;
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (present[i]) entry.grants[i] = proposed[i];
    }
    entry.last_sequence = update.sequence;
    entry.has_update = true;
    const uint64_t ttl_ns =
        static_cast<uint64_t>(update.freshness_ttl_ms) * 1000000ULL;
    const uint64_t now = nowNanos();
    entry.expires_at_ns = ttl_ns > std::numeric_limits<uint64_t>::max() - now
                              ? std::numeric_limits<uint64_t>::max()
                              : now + ttl_ns;
    disposition = gap ? CreditUpdateDisposition::SequenceGap
                      : CreditUpdateDisposition::Applied;
    return Status::OK();
}

Status SenderCreditLedger::tryReserve(const CreditKey& key,
                                      const CreditCharge& charge) {
    std::array<uint64_t, kCreditResourceCount> normalized;
    CHECK_STATUS(normalize(charge, normalized));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return Status::InvalidEntry("credit unavailable" LOC_MARK);
    }
    CHECK_STATUS(requireFresh(it->second));
    auto& entry = it->second;
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (entry.consumed[i] > entry.grants[i] ||
            normalized[i] > entry.grants[i] - entry.consumed[i]) {
            return Status::TooManyRequests("insufficient credit" LOC_MARK);
        }
    }
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        entry.consumed[i] += normalized[i];
    }
    return Status::OK();
}

Status SenderCreditLedger::rollbackReservation(const CreditKey& key,
                                               const CreditCharge& charge) {
    std::array<uint64_t, kCreditResourceCount> normalized;
    CHECK_STATUS(normalize(charge, normalized));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return Status::InvalidEntry("credit session inactive" LOC_MARK);
    }
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (it->second.released[i] > it->second.consumed[i] ||
            normalized[i] > it->second.consumed[i] - it->second.released[i]) {
            return Status::InvalidArgument(
                "credit rollback underflow" LOC_MARK);
        }
    }
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (normalized[i] >
            std::numeric_limits<uint64_t>::max() - it->second.released[i]) {
            return Status::InvalidArgument("credit rollback overflow" LOC_MARK);
        }
        it->second.released[i] += normalized[i];
    }
    return Status::OK();
}

Status SenderCreditLedger::releaseCommitted(const CreditKey& key,
                                            const CreditCharge& charge) {
    std::array<uint64_t, kCreditResourceCount> normalized;
    CHECK_STATUS(normalize(charge, normalized));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return Status::InvalidEntry("credit session inactive" LOC_MARK);
    }
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (it->second.released[i] > it->second.consumed[i] ||
            normalized[i] > it->second.consumed[i] - it->second.released[i]) {
            return Status::InvalidArgument(
                "credit release exceeds committed consumption" LOC_MARK);
        }
    }
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (normalized[i] >
            std::numeric_limits<uint64_t>::max() - it->second.released[i]) {
            return Status::InvalidArgument("credit release overflow" LOC_MARK);
        }
        it->second.released[i] += normalized[i];
    }
    return Status::OK();
}

Status SenderCreditLedger::prepareExchange(
    const CreditKey& key, const CreditCharge& next_charge,
    ReceiverCreditExchangeRequestV1& request) {
    std::array<uint64_t, kCreditResourceCount> normalized;
    CHECK_STATUS(normalize(next_charge, normalized));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return Status::InvalidEntry("credit session inactive" LOC_MARK);
    }
    auto& entry = it->second;
    if (entry.report_sequence == std::numeric_limits<uint64_t>::max()) {
        return Status::InternalError(
            "credit report sequence exhausted" LOC_MARK);
    }

    request = ReceiverCreditExchangeRequestV1{};
    request.expected_receiver_session_id = key.receiver_session;
    request.epoch = entry.epoch;
    request.sender_instance_id = key.sender_instance;
    request.qos_class = key.qos_class;
    request.known_update_sequence = entry.last_sequence;
    request.report_sequence = ++entry.report_sequence;
    request.released_totals.reserve(kCreditResourceCount);
    request.desired_grant_totals.reserve(next_charge.resources.size());
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        const auto resource = static_cast<CreditResource>(i + 1);
        request.released_totals.push_back({resource, entry.released[i]});
        if (normalized[i] == 0) continue;
        if (normalized[i] >
            std::numeric_limits<uint64_t>::max() - entry.consumed[i]) {
            return Status::InvalidArgument(
                "desired credit total overflow" LOC_MARK);
        }
        request.desired_grant_totals.push_back(
            {resource, entry.consumed[i] + normalized[i]});
    }
    return Status::OK();
}

Status SenderCreditLedger::prepareReleaseExchange(
    const CreditKey& key, ReceiverCreditExchangeRequestV1& request) {
    std::lock_guard lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return Status::InvalidEntry("credit session inactive" LOC_MARK);
    }
    auto& entry = it->second;
    if (entry.report_sequence == std::numeric_limits<uint64_t>::max()) {
        return Status::InternalError(
            "credit report sequence exhausted" LOC_MARK);
    }

    request = ReceiverCreditExchangeRequestV1{};
    request.expected_receiver_session_id = key.receiver_session;
    request.epoch = entry.epoch;
    request.sender_instance_id = key.sender_instance;
    request.qos_class = key.qos_class;
    request.known_update_sequence = entry.last_sequence;
    request.report_sequence = ++entry.report_sequence;
    request.released_totals.reserve(kCreditResourceCount);
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        const auto resource = static_cast<CreditResource>(i + 1);
        request.released_totals.push_back({resource, entry.released[i]});
        if (entry.consumed[i] != 0) {
            request.desired_grant_totals.push_back(
                {resource, entry.consumed[i]});
        }
    }
    if (request.desired_grant_totals.empty()) {
        return Status::InvalidEntry(
            "credit release exchange has no consumption" LOC_MARK);
    }
    return Status::OK();
}

Status SenderCreditLedger::available(const CreditKey& key,
                                     CreditResource resource,
                                     uint64_t& value) const {
    size_t index = 0;
    CHECK_STATUS(resourceIndex(resource, index));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return Status::InvalidEntry("credit unavailable" LOC_MARK);
    }
    CHECK_STATUS(requireFresh(it->second));
    value = it->second.grants[index] - it->second.consumed[index];
    return Status::OK();
}

Status SenderCreditLedger::consumed(const CreditKey& key,
                                    CreditResource resource,
                                    uint64_t& value) const {
    size_t index = 0;
    CHECK_STATUS(resourceIndex(resource, index));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return Status::InvalidEntry("credit session inactive" LOC_MARK);
    }
    value = it->second.consumed[index];
    return Status::OK();
}

Status SenderCreditLedger::released(const CreditKey& key,
                                    CreditResource resource,
                                    uint64_t& value) const {
    size_t index = 0;
    CHECK_STATUS(resourceIndex(resource, index));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return Status::InvalidEntry("credit session inactive" LOC_MARK);
    }
    value = it->second.released[index];
    return Status::OK();
}

ReceiverCreditAuthority::ReceiverCreditAuthority(ReceiverSessionId session,
                                                 uint64_t epoch,
                                                 ReceiverCreditLimits limits)
    : session_(session), epoch_(epoch), limits_(std::move(limits)) {
    if (session_.empty() || epoch_ == 0) {
        status_ = Status::InvalidArgument(
            "invalid receiver credit authority identity" LOC_MARK);
    } else {
        status_ = validateLimits(limits_);
    }
}

Status ReceiverCreditAuthority::validateLimits(
    const ReceiverCreditLimits& limits) {
    if (limits.freshness_ttl_ms == 0 || limits.max_senders == 0 ||
        limits.max_entries < limits.max_senders) {
        return Status::InvalidArgument(
            "invalid receiver credit limits" LOC_MARK);
    }
    bool any_capacity = false;
    for (const auto capacity : limits.capacity) any_capacity |= capacity != 0;
    if (!any_capacity) {
        return Status::InvalidArgument(
            "receiver credit capacity is empty" LOC_MARK);
    }
    return Status::OK();
}

ReceiverCreditAdvertV1 ReceiverCreditAuthority::advert() const {
    ReceiverCreditAdvertV1 result;
    result.receiver_session_id = session_;
    result.epoch = epoch_;
    result.freshness_ttl_ms = limits_.freshness_ttl_ms;
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (limits_.capacity[i] != 0) {
            const auto resource = static_cast<CreditResource>(i + 1);
            result.resources.push_back(resource);
            result.capacities.push_back({resource, limits_.capacity[i]});
        }
    }
    return result;
}

Status ReceiverCreditAuthority::normalizeCounters(
    const std::vector<CreditCounter>& counters,
    std::array<uint64_t, kCreditResourceCount>& normalized,
    std::array<bool, kCreditResourceCount>& present) {
    normalized.fill(0);
    present.fill(false);
    if (counters.size() > kCreditResourceCount) {
        return Status::InvalidArgument("too many credit counters" LOC_MARK);
    }
    for (const auto& counter : counters) {
        size_t index = 0;
        CHECK_STATUS(indexFor(counter.resource, index));
        if (present[index]) {
            return Status::InvalidArgument(
                "duplicate credit counter resource" LOC_MARK);
        }
        present[index] = true;
        normalized[index] = counter.total;
    }
    return Status::OK();
}

void ReceiverCreditAuthority::fillReply(
    const CreditKey& key, const Entry& entry,
    ReceiverCreditExchangeReplyV1& reply) const {
    reply = ReceiverCreditExchangeReplyV1{};
    reply.sender_instance_id = key.sender_instance;
    reply.update.qos_class = key.qos_class;
    reply.update.receiver_session_id = session_;
    reply.update.epoch = epoch_;
    reply.update.sequence = entry.update_sequence;
    reply.update.freshness_ttl_ms = limits_.freshness_ttl_ms;
    reply.update.grants.reserve(kCreditResourceCount);
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        reply.update.grants.push_back(
            {static_cast<CreditResource>(i + 1), entry.grants[i]});
    }
}

Status ReceiverCreditAuthority::exchange(
    const ReceiverCreditExchangeRequestV1& request,
    ReceiverCreditExchangeReplyV1& reply) {
    CHECK_STATUS(status_);
    if (request.schema_version != kReceiverCreditProtocolVersion ||
        request.flags != 0 || request.epoch != epoch_ ||
        !(request.expected_receiver_session_id == session_) ||
        request.sender_instance_id.empty() || request.qos_class > 2 ||
        request.report_sequence == 0 || request.desired_grant_totals.empty()) {
        return Status::InvalidArgument(
            "invalid receiver credit exchange identity/header" LOC_MARK);
    }

    std::array<uint64_t, kCreditResourceCount> released{}, desired{};
    std::array<bool, kCreditResourceCount> released_present{},
        desired_present{};
    CHECK_STATUS(
        normalizeCounters(request.released_totals, released, released_present));
    CHECK_STATUS(normalizeCounters(request.desired_grant_totals, desired,
                                   desired_present));

    CreditKey key{request.expected_receiver_session_id,
                  request.sender_instance_id, request.qos_class};
    std::lock_guard lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        if (request.known_update_sequence != 0) {
            return Status::InvalidArgument(
                "new sender knows an impossible credit update" LOC_MARK);
        }
        for (size_t i = 0; i < kCreditResourceCount; ++i) {
            if (released_present[i] && released[i] != 0) {
                return Status::InvalidArgument(
                    "new sender reports impossible credit release" LOC_MARK);
            }
        }
        if (entries_.size() >= limits_.max_entries) {
            return Status::TooManyRequests(
                "receiver credit replay-history limit" LOC_MARK);
        }
        it = entries_.emplace(key, Entry{}).first;
    }
    auto& entry = it->second;
    if (request.known_update_sequence > entry.update_sequence) {
        return Status::InvalidArgument(
            "sender knows an impossible credit update" LOC_MARK);
    }
    if (request.report_sequence <= entry.last_report_sequence) {
        fillReply(key, entry, reply);
        return Status::OK();
    }
    if (entry.update_sequence == std::numeric_limits<uint64_t>::max()) {
        return Status::InternalError(
            "receiver credit update sequence exhausted" LOC_MARK);
    }

    // Validate the entire cumulative release before mutating any resource.
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (!released_present[i]) continue;
        if (released[i] < entry.released[i] || released[i] > entry.grants[i]) {
            return Status::InvalidArgument(
                "invalid cumulative credit release" LOC_MARK);
        }
    }
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (!released_present[i]) continue;
        const uint64_t delta = released[i] - entry.released[i];
        if (delta > outstanding_[i]) {
            return Status::InternalError(
                "receiver credit accounting underflow" LOC_MARK);
        }
    }

    // Apply releases first, then atomically decide whether every requested
    // resource can reach its absolute desired total.
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (!released_present[i]) continue;
        outstanding_[i] -= released[i] - entry.released[i];
        entry.released[i] = released[i];
    }

    std::array<uint64_t, kCreditResourceCount> grant_delta{};
    bool can_grant_all = true;
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (!desired_present[i] || desired[i] <= entry.grants[i]) continue;
        grant_delta[i] = desired[i] - entry.grants[i];
        const uint64_t free = limits_.capacity[i] - outstanding_[i];
        if (grant_delta[i] > free) can_grant_all = false;
    }
    const bool requests_new_grant =
        std::any_of(grant_delta.begin(), grant_delta.end(),
                    [](uint64_t delta) { return delta != 0; });
    if (can_grant_all && requests_new_grant) {
        std::unordered_set<SenderInstanceId, SenderInstanceIdHash>
            active_senders;
        active_senders.reserve(std::min(entries_.size(), limits_.max_senders));
        for (const auto& [entry_key, tracked] : entries_) {
            bool has_outstanding = false;
            for (size_t i = 0; i < kCreditResourceCount; ++i) {
                has_outstanding |= tracked.grants[i] > tracked.released[i];
            }
            if (has_outstanding) {
                active_senders.insert(entry_key.sender_instance);
            }
        }
        if (!active_senders.count(request.sender_instance_id) &&
            active_senders.size() >= limits_.max_senders) {
            can_grant_all = false;
        }
    }
    if (can_grant_all) {
        for (size_t i = 0; i < kCreditResourceCount; ++i) {
            entry.grants[i] += grant_delta[i];
            outstanding_[i] += grant_delta[i];
        }
    }

    entry.last_report_sequence = request.report_sequence;
    ++entry.update_sequence;
    fillReply(key, entry, reply);
    return Status::OK();
}

Status ReceiverCreditAuthority::outstanding(CreditResource resource,
                                            uint64_t& value) const {
    size_t index = 0;
    CHECK_STATUS(indexFor(resource, index));
    std::lock_guard lock(mutex_);
    value = outstanding_[index];
    return Status::OK();
}

}  // namespace mooncake::tent

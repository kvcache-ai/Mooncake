// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit.h"

namespace mooncake::tent {

size_t CreditKeyHash::operator()(const CreditKey& k) const noexcept {
    size_t h = std::hash<uint64_t>{}(k.receiver_session.high);
    auto mix = [&h](uint64_t v) {
        h ^= std::hash<uint64_t>{}(v) + 0x9e3779b97f4a7c15ULL + (h << 6) +
             (h >> 2);
    };
    mix(k.receiver_session.low);
    mix(k.sender_peer);
    mix(k.qos_class);
    return h;
}

Status SenderCreditLedger::resourceIndex(CreditResource r, size_t& i) {
    auto raw = static_cast<uint16_t>(r);
    if (raw < 1 || raw > kCreditResourceCount)
        return Status::InvalidArgument("unknown credit resource" LOC_MARK);
    i = raw - 1;
    return Status::OK();
}

Status SenderCreditLedger::normalize(
    const CreditCharge& c, std::array<uint64_t, kCreditResourceCount>& out) {
    out.fill(0);
    if (c.resources.empty())
        return Status::InvalidArgument("empty credit charge" LOC_MARK);
    for (auto [r, amount] : c.resources) {
        size_t i = 0;
        CHECK_STATUS(resourceIndex(r, i));
        if (!amount || out[i])
            return Status::InvalidArgument(
                "zero or duplicate credit charge" LOC_MARK);
        out[i] = amount;
    }
    return Status::OK();
}

Status SenderCreditLedger::activate(const CreditKey& k, uint64_t epoch) {
    if (!epoch) return Status::InvalidArgument("zero credit epoch" LOC_MARK);
    std::lock_guard lock(mutex_);
    auto existing = entries_.find(k);
    if (existing != entries_.end()) {
        if (epoch < existing->second.epoch)
            return Status::InvalidEntry("stale credit activation" LOC_MARK);
        if (epoch == existing->second.epoch) return Status::OK();
    } else if (entries_.size() >= max_entries_) {
        return Status::TooManyRequests("credit ledger entry limit" LOC_MARK);
    }
    Entry e;
    e.epoch = epoch;
    entries_.insert_or_assign(k, e);  // fences all prior-epoch unused state
    return Status::OK();
}

Status SenderCreditLedger::applyUpdate(const CreditKey& k,
                                       const ReceiverCreditUpdateV1& u,
                                       CreditUpdateDisposition& disposition) {
    if (u.schema_version != 1 || !u.epoch || !u.sequence)
        return Status::InvalidArgument("invalid credit update header" LOC_MARK);
    if (!(u.receiver_session_id == k.receiver_session) ||
        u.qos_class != k.qos_class || u.grants.size() > kCreditResourceCount)
        return Status::InvalidArgument("credit update identity/size" LOC_MARK);
    std::array<uint64_t, kCreditResourceCount> proposed{};
    std::array<bool, kCreditResourceCount> present{};
    for (auto a : u.grants) {
        size_t i = 0;
        CHECK_STATUS(resourceIndex(a.resource, i));
        if (present[i])
            return Status::InvalidArgument("duplicate grant resource" LOC_MARK);
        present[i] = true;
        proposed[i] = a.grant_total;
    }
    std::lock_guard lock(mutex_);
    auto it = entries_.find(k);
    if (it == entries_.end() || it->second.epoch != u.epoch)
        return Status::InvalidEntry("inactive or stale credit epoch" LOC_MARK);
    auto& e = it->second;
    if (e.has_update && u.sequence <= e.last_sequence) {
        disposition = CreditUpdateDisposition::DuplicateOrOld;
        return Status::OK();
    }
    for (size_t i = 0; i < kCreditResourceCount; ++i)
        if (present[i] &&
            (proposed[i] < e.grants[i] || proposed[i] < e.consumed[i]))
            return Status::InvalidArgument(
                "decreasing or under-consumed grant" LOC_MARK);
    bool gap = e.has_update && u.sequence > e.last_sequence + 1;
    for (size_t i = 0; i < kCreditResourceCount; ++i)
        if (present[i]) e.grants[i] = proposed[i];
    e.last_sequence = u.sequence;
    e.has_update = true;
    disposition = gap ? CreditUpdateDisposition::SequenceGap
                      : CreditUpdateDisposition::Applied;
    return Status::OK();
}

Status SenderCreditLedger::tryReserve(const CreditKey& k,
                                      const CreditCharge& c) {
    std::array<uint64_t, kCreditResourceCount> n;
    CHECK_STATUS(normalize(c, n));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(k);
    if (it == entries_.end() || !it->second.has_update)
        return Status::InvalidEntry("credit unavailable" LOC_MARK);
    auto& e = it->second;
    for (size_t i = 0; i < kCreditResourceCount; ++i)
        if (e.consumed[i] > e.grants[i] || n[i] > e.grants[i] - e.consumed[i])
            return Status::TooManyRequests("insufficient credit" LOC_MARK);
    for (size_t i = 0; i < kCreditResourceCount; ++i) e.consumed[i] += n[i];
    return Status::OK();
}

Status SenderCreditLedger::rollbackReservation(const CreditKey& k,
                                               const CreditCharge& c) {
    std::array<uint64_t, kCreditResourceCount> n;
    CHECK_STATUS(normalize(c, n));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(k);
    if (it == entries_.end())
        return Status::InvalidEntry("credit session inactive" LOC_MARK);
    for (size_t i = 0; i < kCreditResourceCount; ++i)
        if (n[i] > it->second.consumed[i])
            return Status::InvalidArgument(
                "credit rollback underflow" LOC_MARK);
    for (size_t i = 0; i < kCreditResourceCount; ++i)
        it->second.consumed[i] -= n[i];
    return Status::OK();
}

Status SenderCreditLedger::available(const CreditKey& k, CreditResource r,
                                     uint64_t& v) const {
    size_t i = 0;
    CHECK_STATUS(resourceIndex(r, i));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(k);
    if (it == entries_.end() || !it->second.has_update)
        return Status::InvalidEntry("credit unavailable" LOC_MARK);
    v = it->second.grants[i] - it->second.consumed[i];
    return Status::OK();
}

Status SenderCreditLedger::consumed(const CreditKey& k, CreditResource r,
                                    uint64_t& v) const {
    size_t i = 0;
    CHECK_STATUS(resourceIndex(r, i));
    std::lock_guard lock(mutex_);
    auto it = entries_.find(k);
    if (it == entries_.end())
        return Status::InvalidEntry("credit session inactive" LOC_MARK);
    v = it->second.consumed[i];
    return Status::OK();
}

}  // namespace mooncake::tent

// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_control.h"

#include <algorithm>
#include <array>

namespace mooncake::tent {

CreditCapabilityState::CreditCapabilityState(CreditRolloutMode mode)
    : mode_(mode),
      state_(mode == CreditRolloutMode::Disabled
                 ? CreditPeerState::Disabled
                 : CreditPeerState::Negotiating) {}

Status CreditCapabilityState::beginNegotiation() {
    if (mode_ == CreditRolloutMode::Disabled) return Status::OK();
    state_ = CreditPeerState::Negotiating;
    version_ = 0;
    return Status::OK();
}

Status CreditCapabilityState::completeNegotiation(
    const std::vector<uint16_t>& peer_versions) {
    if (mode_ == CreditRolloutMode::Disabled) return Status::OK();
    if (state_ != CreditPeerState::Negotiating)
        return Status::InvalidEntry("credit peer is not negotiating" LOC_MARK);
    auto supported = std::find(peer_versions.begin(), peer_versions.end(), 1);
    if (supported != peer_versions.end()) {
        version_ = 1;
        state_ = CreditPeerState::Active;
        return Status::OK();
    }
    if (mode_ == CreditRolloutMode::Optional) {
        state_ = CreditPeerState::Legacy;
        return Status::OK();
    }
    state_ = CreditPeerState::Failed;
    return Status::NotImplemented(
        "receiver credit is required but unsupported" LOC_MARK);
}

Status CreditCapabilityState::markStale() {
    if (state_ != CreditPeerState::Active)
        return Status::InvalidEntry(
            "only active credit can become stale" LOC_MARK);
    state_ = CreditPeerState::Stale;
    return Status::OK();
}

Status CreditCapabilityState::refresh(uint16_t version) {
    if (state_ != CreditPeerState::Stale || version != version_)
        return Status::InvalidArgument("invalid stale credit refresh" LOC_MARK);
    state_ = CreditPeerState::Active;
    return Status::OK();
}

Status BoundedCreditUpdateInbox::tryPublish(CreditControlEnvelope envelope) {
    if (capacity_ == 0)
        return Status::TooManyRequests("credit update inbox disabled" LOC_MARK);
    std::lock_guard lock(mutex_);
    if (queue_.size() >= capacity_)
        return Status::TooManyRequests("credit update inbox full" LOC_MARK);
    queue_.push_back(std::move(envelope));
    return Status::OK();
}

size_t BoundedCreditUpdateInbox::drain(
    std::vector<CreditControlEnvelope>& output, size_t max_updates) {
    std::lock_guard lock(mutex_);
    size_t count = std::min(max_updates, queue_.size());
    output.reserve(output.size() + count);
    for (size_t i = 0; i < count; ++i) {
        output.push_back(std::move(queue_.front()));
        queue_.pop_front();
    }
    return count;
}

size_t BoundedCreditUpdateInbox::size() const {
    std::lock_guard lock(mutex_);
    return queue_.size();
}

namespace {
constexpr uint32_t kCreditMagic = 0x54435231;      // "TCR1"
constexpr uint32_t kCapabilityMagic = 0x54434331;  // "TCC1"
constexpr uint32_t kActivationMagic = 0x54434131;  // "TCA1"
void append16(std::string& out, uint16_t v) {
    out.push_back(static_cast<char>(v >> 8));
    out.push_back(static_cast<char>(v));
}
void append32(std::string& out, uint32_t v) {
    for (int shift = 24; shift >= 0; shift -= 8)
        out.push_back(static_cast<char>(v >> shift));
}
void append64(std::string& out, uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8)
        out.push_back(static_cast<char>(v >> shift));
}
uint16_t read16(std::string_view in, size_t& p) {
    uint16_t v = 0;
    for (int i = 0; i < 2; ++i) v = (v << 8) | uint8_t(in[p++]);
    return v;
}
uint32_t read32(std::string_view in, size_t& p) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v = (v << 8) | uint8_t(in[p++]);
    return v;
}
uint64_t read64(std::string_view in, size_t& p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | uint8_t(in[p++]);
    return v;
}
}  // namespace

Status CreditCapabilityCodecV1::encode(const std::vector<uint16_t>& versions,
                                       std::string& wire) {
    if (versions.size() > kMaxVersions)
        return Status::InvalidArgument(
            "too many receiver credit versions" LOC_MARK);
    std::vector<uint16_t> encoded_versions;
    encoded_versions.reserve(versions.size());
    std::string encoded;
    encoded.reserve(kHeaderBytes + versions.size() * sizeof(uint16_t));
    append32(encoded, kCapabilityMagic);
    append16(encoded, static_cast<uint16_t>(versions.size()));
    append16(encoded, 0);
    for (uint16_t version : versions) {
        if (version == 0 ||
            std::find(encoded_versions.begin(), encoded_versions.end(),
                      version) != encoded_versions.end())
            return Status::InvalidArgument(
                "invalid or duplicate receiver credit version" LOC_MARK);
        encoded_versions.push_back(version);
        append16(encoded, version);
    }
    wire.swap(encoded);
    return Status::OK();
}

Status CreditCapabilityCodecV1::decode(std::string_view wire,
                                       std::vector<uint16_t>& versions) {
    if (wire.size() < kHeaderBytes || wire.size() > kMaxWireBytes)
        return Status::InvalidArgument(
            "invalid receiver credit capability length" LOC_MARK);
    size_t p = 0;
    if (read32(wire, p) != kCapabilityMagic)
        return Status::InvalidArgument(
            "invalid receiver credit capability magic" LOC_MARK);
    uint16_t count = read16(wire, p);
    uint16_t reserved = read16(wire, p);
    if (reserved != 0 || count > kMaxVersions ||
        wire.size() != kHeaderBytes + count * sizeof(uint16_t))
        return Status::InvalidArgument(
            "invalid receiver credit capability header" LOC_MARK);
    std::vector<uint16_t> decoded;
    decoded.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
        uint16_t version = read16(wire, p);
        if (version == 0 ||
            std::find(decoded.begin(), decoded.end(), version) != decoded.end())
            return Status::InvalidArgument(
                "invalid receiver credit capability version" LOC_MARK);
        decoded.push_back(version);
    }
    versions = std::move(decoded);
    return Status::OK();
}

Status CreditActivationCodecV1::encode(const CreditActivationV1& activation,
                                       std::string& wire) {
    if (activation.schema_version != 1 || activation.chosen_version != 1 ||
        (!activation.receiver_session_id.high &&
         !activation.receiver_session_id.low) ||
        !activation.epoch)
        return Status::InvalidArgument(
            "invalid receiver credit activation" LOC_MARK);
    std::string encoded;
    encoded.reserve(kWireBytes);
    append32(encoded, kActivationMagic);
    append16(encoded, activation.schema_version);
    append16(encoded, activation.chosen_version);
    append64(encoded, activation.receiver_session_id.high);
    append64(encoded, activation.receiver_session_id.low);
    append64(encoded, activation.epoch);
    append32(encoded, activation.freshness_ttl_ms);
    append32(encoded, 0);
    wire.swap(encoded);
    return Status::OK();
}

Status CreditActivationCodecV1::decode(std::string_view wire,
                                       CreditActivationV1& activation) {
    if (wire.size() != kWireBytes)
        return Status::InvalidArgument(
            "invalid receiver credit activation length" LOC_MARK);
    size_t p = 0;
    if (read32(wire, p) != kActivationMagic)
        return Status::InvalidArgument(
            "invalid receiver credit activation magic" LOC_MARK);
    CreditActivationV1 decoded;
    decoded.schema_version = read16(wire, p);
    decoded.chosen_version = read16(wire, p);
    decoded.receiver_session_id.high = read64(wire, p);
    decoded.receiver_session_id.low = read64(wire, p);
    decoded.epoch = read64(wire, p);
    decoded.freshness_ttl_ms = read32(wire, p);
    uint32_t reserved = read32(wire, p);
    if (decoded.schema_version != 1 || decoded.chosen_version != 1 ||
        (!decoded.receiver_session_id.high &&
         !decoded.receiver_session_id.low) ||
        !decoded.epoch || reserved != 0)
        return Status::InvalidArgument(
            "invalid receiver credit activation header" LOC_MARK);
    activation = decoded;
    return Status::OK();
}

size_t CreditPeerContextTable::LookupKeyHash::operator()(
    const LookupKey& key) const noexcept {
    size_t h = std::hash<uint64_t>{}(key.target_id);
    h ^= std::hash<uint32_t>{}(key.qos_class) + 0x9e3779b97f4a7c15ULL +
         (h << 6) + (h >> 2);
    return h;
}

Status CreditPeerContextTable::activate(uint64_t target_id,
                                        uint64_t sender_peer,
                                        uint32_t qos_class,
                                        const CreditActivationV1& activation) {
    if (!sender_peer || activation.schema_version != 1 ||
        activation.chosen_version != 1 ||
        (!activation.receiver_session_id.high &&
         !activation.receiver_session_id.low) ||
        !activation.epoch)
        return Status::InvalidArgument(
            "invalid receiver credit peer context" LOC_MARK);
    LookupKey lookup_key{target_id, qos_class};
    CreditPeerContextSnapshot next{
        {activation.receiver_session_id, sender_peer, qos_class},
        activation.epoch,
        activation.freshness_ttl_ms};
    std::lock_guard lock(mutex_);
    auto it = contexts_.find(lookup_key);
    if (it == contexts_.end()) {
        if (contexts_.size() >= max_entries_)
            return Status::TooManyRequests(
                "receiver credit peer context table full" LOC_MARK);
        contexts_.emplace(lookup_key, next);
        return Status::OK();
    }
    const auto& current = it->second;
    if (current.key.sender_peer != sender_peer)
        return Status::InvalidEntry(
            "receiver credit sender identity changed" LOC_MARK);
    if (current.key.receiver_session == activation.receiver_session_id &&
        activation.epoch < current.epoch)
        return Status::InvalidEntry(
            "stale receiver credit peer activation" LOC_MARK);
    if (current.key.receiver_session == activation.receiver_session_id &&
        activation.epoch == current.epoch) {
        return Status::OK();
    }
    it->second = next;
    return Status::OK();
}

Status CreditPeerContextTable::lookup(
    uint64_t target_id, uint32_t qos_class,
    CreditPeerContextSnapshot& snapshot) const {
    std::lock_guard lock(mutex_);
    auto it = contexts_.find({target_id, qos_class});
    if (it == contexts_.end())
        return Status::InvalidEntry(
            "receiver credit peer context not found" LOC_MARK);
    snapshot = it->second;
    return Status::OK();
}

Status CreditPeerContextTable::deactivate(
    uint64_t target_id, uint32_t qos_class,
    const ReceiverSessionId& receiver_session, uint64_t epoch) {
    std::lock_guard lock(mutex_);
    auto it = contexts_.find({target_id, qos_class});
    if (it == contexts_.end()) return Status::OK();
    if (!(it->second.key.receiver_session == receiver_session) ||
        it->second.epoch != epoch)
        return Status::InvalidEntry(
            "receiver credit peer cleanup is stale" LOC_MARK);
    contexts_.erase(it);
    return Status::OK();
}

size_t CreditPeerContextTable::size() const {
    std::lock_guard lock(mutex_);
    return contexts_.size();
}

Status ReceiverCreditCodecV1::encode(const ReceiverCreditUpdateV1& u,
                                     std::string& wire) {
    if (u.schema_version != 1 || !u.epoch || !u.sequence ||
        u.grants.size() > kCreditResourceCount)
        return Status::InvalidArgument("invalid credit wire update" LOC_MARK);
    std::array<bool, kCreditResourceCount> present{};
    for (const auto& grant : u.grants) {
        auto raw = static_cast<uint16_t>(grant.resource);
        if (raw < 1 || raw > kCreditResourceCount || present[raw - 1])
            return Status::InvalidArgument(
                "invalid or duplicate wire resource" LOC_MARK);
        present[raw - 1] = true;
    }
    std::string encoded;
    encoded.reserve(kHeaderBytes + u.grants.size() * kGrantBytes);
    append32(encoded, kCreditMagic);
    append16(encoded, u.schema_version);
    append16(encoded, u.flags);
    append32(encoded, u.qos_class);
    append64(encoded, u.receiver_session_id.high);
    append64(encoded, u.receiver_session_id.low);
    append64(encoded, u.epoch);
    append64(encoded, u.sequence);
    append32(encoded, u.freshness_ttl_ms);
    append16(encoded, static_cast<uint16_t>(u.grants.size()));
    append16(encoded, 0);
    for (const auto& grant : u.grants) {
        append16(encoded, static_cast<uint16_t>(grant.resource));
        append16(encoded, 0);
        append64(encoded, grant.grant_total);
    }
    wire.swap(encoded);  // do not mutate output on validation failure
    return Status::OK();
}

Status ReceiverCreditCodecV1::decode(std::string_view wire,
                                     ReceiverCreditUpdateV1& update) {
    if (wire.size() < kHeaderBytes || wire.size() > kMaxWireBytes)
        return Status::InvalidArgument("invalid credit wire length" LOC_MARK);
    size_t p = 0;
    if (read32(wire, p) != kCreditMagic)
        return Status::InvalidArgument("invalid credit wire magic" LOC_MARK);
    ReceiverCreditUpdateV1 decoded;
    decoded.schema_version = read16(wire, p);
    decoded.flags = read16(wire, p);
    decoded.qos_class = read32(wire, p);
    decoded.receiver_session_id.high = read64(wire, p);
    decoded.receiver_session_id.low = read64(wire, p);
    decoded.epoch = read64(wire, p);
    decoded.sequence = read64(wire, p);
    decoded.freshness_ttl_ms = read32(wire, p);
    uint16_t count = read16(wire, p);
    uint16_t reserved = read16(wire, p);
    if (decoded.schema_version != 1 || !decoded.epoch || !decoded.sequence ||
        reserved != 0 || count > kCreditResourceCount ||
        wire.size() != kHeaderBytes + count * kGrantBytes)
        return Status::InvalidArgument("invalid credit wire header" LOC_MARK);
    std::array<bool, kCreditResourceCount> present{};
    decoded.grants.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
        uint16_t raw = read16(wire, p);
        uint16_t grant_reserved = read16(wire, p);
        uint64_t total = read64(wire, p);
        if (raw < 1 || raw > kCreditResourceCount || present[raw - 1] ||
            grant_reserved != 0)
            return Status::InvalidArgument(
                "invalid credit wire grant" LOC_MARK);
        present[raw - 1] = true;
        decoded.grants.push_back({static_cast<CreditResource>(raw), total});
    }
    update = std::move(decoded);  // atomic decode result
    return Status::OK();
}

Status ReceiverCreditIngress::tryAccept(std::string_view wire) {
    ReceiverCreditUpdateV1 decoded;
    CHECK_STATUS(ReceiverCreditCodecV1::decode(wire, decoded));
    if (!(decoded.receiver_session_id == key_.receiver_session) ||
        decoded.qos_class != key_.qos_class || decoded.epoch != epoch_)
        return Status::InvalidEntry(
            "credit update does not match active ingress" LOC_MARK);
    return inbox_.tryPublish({key_, std::move(decoded)});
}

}  // namespace mooncake::tent

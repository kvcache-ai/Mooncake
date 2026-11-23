// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_destination_registry.h"

#include <algorithm>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <set>
#include <tuple>
#include <utility>

namespace mooncake::ssd_offload {
namespace {
bool ValidPeer(PeerIdentity peer) {
    return (peer.client_id_high || peer.client_id_low) && peer.boot_epoch;
}
auto PeerTuple(PeerIdentity peer) {
    return std::make_tuple(peer.client_id_high, peer.client_id_low,
                           peer.boot_epoch);
}
struct PeerLess {
    bool operator()(PeerIdentity a, PeerIdentity b) const {
        return PeerTuple(a) < PeerTuple(b);
    }
};
struct AttemptLess {
    bool operator()(const AttemptIdentity& a, const AttemptIdentity& b) const {
        return std::make_tuple(PeerTuple(a.owner), a.request_id, a.attempt_id) <
               std::make_tuple(PeerTuple(b.owner), b.request_id, b.attempt_id);
    }
};
bool Overlap(std::pair<uint64_t, uint64_t> a, std::pair<uint64_t, uint64_t> b) {
    return a.first < b.second && b.first < a.second;
}
bool ValidLimits(const DestinationRegistryLimits& limits) {
    return limits.max_regions && limits.max_regions <= 65536 &&
           limits.max_tracked_attempts &&
           limits.max_tracked_attempts <= 65536 && limits.max_owner_sessions &&
           limits.max_owner_sessions <= 65536 && limits.max_active_fragments &&
           limits.max_active_fragments <= 1048576 && limits.max_active_bytes;
}
struct Entry {
    DestinationRegion region;
    DestinationBinding binding;
    std::shared_ptr<void> lifetime;
    size_t references{0};
    bool closing{false};
    bool retiring{false};
};
}  // namespace

struct DestinationRegistry::Record {
    AttemptIdentity identity;
    DestinationAttemptState stage{DestinationAttemptState::kPrepared};
    uint64_t bytes{0}, valid_bytes{0};
    size_t fragments{0};
    bool publishable{false};
    std::vector<std::shared_ptr<Entry>> entries;
    std::vector<ResolvedDestination> views;
    std::vector<std::pair<uint64_t, uint64_t>> intervals;
};

struct DestinationRegistry::State : std::enable_shared_from_this<State> {
    struct Slot {
        uint64_t generation{0};
        std::shared_ptr<Entry> entry;
    };
    State(PeerIdentity peer, std::string endpoint,
          DestinationRegistryLimits bounds)
        : requester(peer),
          endpoint(std::move(endpoint)),
          limits(bounds),
          slots(bounds.max_regions) {}

    DestinationError Find(const DestinationRegion& reference,
                          std::shared_ptr<Entry>& result) const {
        if (reference.requester != requester)
            return DestinationError::kStaleRegion;
        if (!reference.region_id || reference.region_id > slots.size())
            return DestinationError::kNotFound;
        const auto& slot = slots[reference.region_id - 1];
        if (!slot.generation) return DestinationError::kNotFound;
        if (reference.generation != slot.generation)
            return DestinationError::kStaleRegion;
        if (!slot.entry) return DestinationError::kNotFound;
        // A credential authorizes this exact region and generation. It does
        // not make previously accepted DMA disappear on revocation.
        const auto& credential = slot.entry->region.credential;
        if ((reference.credential.high ^ credential.high) |
            (reference.credential.low ^ credential.low))
            return DestinationError::kUnauthorized;
        result = slot.entry;
        return DestinationError::kOk;
    }
    void Release(Record& record, bool publishable, uint64_t valid_bytes) {
        if (record.stage == DestinationAttemptState::kExposed ||
            record.stage == DestinationAttemptState::kQuarantined)
            --exposed;
        for (auto& entry : record.entries) --entry->references;
        active_bytes -= record.bytes;
        active_fragments -= record.fragments;
        --active_attempts;
        record.stage = DestinationAttemptState::kDrained;
        record.publishable = publishable;
        record.valid_bytes = valid_bytes;
        // Keep only the bounded terminal summary, not completed requests'
        // large fragment arrays after their active credits are returned.
        std::vector<std::shared_ptr<Entry>>().swap(record.entries);
        std::vector<ResolvedDestination>().swap(record.views);
        std::vector<std::pair<uint64_t, uint64_t>>().swap(record.intervals);
        // Preserve registrations even if both the registry and caller's
        // handle disappear after exposure. Drop that hold only on actual
        // drain, never because a software timeout or destructor ran.
        if (!exposed) drain_hold.reset();
    }
    DestinationError Finish(Record& record, const DestinationDrainAck& ack) {
        if (record.identity != ack.attempt || ack.requester != requester)
            return DestinationError::kWrongAttempt;
        if (record.stage == DestinationAttemptState::kDrained)
            return DestinationError::kDuplicate;
        if (!ack.dma_drained) return DestinationError::kNotDrained;
        if (ack.valid_bytes > record.bytes ||
            (ack.outcome == AttemptOutcome::kSucceeded &&
             ack.valid_bytes != record.bytes) ||
            (ack.outcome == AttemptOutcome::kRejected &&
             ack.valid_bytes != 0) ||
            (ack.outcome != AttemptOutcome::kSucceeded &&
             ack.outcome != AttemptOutcome::kFailed &&
             ack.outcome != AttemptOutcome::kCancelled &&
             ack.outcome != AttemptOutcome::kRejected))
            return DestinationError::kInvalidArgument;
        if (record.stage == DestinationAttemptState::kPrepared &&
            (ack.valid_bytes || (ack.outcome != AttemptOutcome::kRejected &&
                                 ack.outcome != AttemptOutcome::kCancelled)))
            return DestinationError::kInvalidArgument;
        Release(record,
                ack.outcome == AttemptOutcome::kSucceeded &&
                    record.stage == DestinationAttemptState::kExposed,
                ack.valid_bytes);
        return DestinationError::kOk;
    }
    DestinationAttemptSnapshot Snapshot(const Record& record) const {
        return {DestinationError::kOk, record.stage, record.bytes,
                record.valid_bytes, record.publishable};
    }

    PeerIdentity requester;
    const std::string endpoint;
    const DestinationRegistryLimits limits;
    mutable std::mutex mutex;
    std::vector<Slot> slots;
    std::map<AttemptIdentity, std::shared_ptr<Record>, AttemptLess> records;
    std::map<PeerIdentity, uint64_t, PeerLess> replay_floors;
    size_t active_attempts{0}, active_fragments{0}, exposed{0};
    uint64_t active_bytes{0};
    std::shared_ptr<State> drain_hold;
};

DestinationRegistry::DestinationRegistry(PeerIdentity peer,
                                         std::string endpoint,
                                         DestinationRegistryLimits limits) {
    if (!ValidPeer(peer) || endpoint.empty() || endpoint.size() > 4096 ||
        endpoint.find('\0') != std::string::npos || !ValidLimits(limits))
        throw std::invalid_argument(
            "Invalid SSD destination registry identity or limits");
    state_ = std::make_shared<State>(peer, std::move(endpoint), limits);
}
DestinationRegistry::~DestinationRegistry() = default;

DestinationRegistry::RegistrationResult DestinationRegistry::Register(
    DestinationBinding binding, WriteCredential credential,
    std::shared_ptr<void> lifetime) {
    if (!binding.logical_base || !binding.logical_bytes ||
        !binding.registered_base || !binding.registered_bytes || !lifetime ||
        (!credential.high && !credential.low) ||
        (binding.memory != DestinationMemory::kHost &&
         binding.memory != DestinationMemory::kDevice))
        return {DestinationError::kInvalidArgument, {}};
    const uint64_t maximum = std::numeric_limits<uint64_t>::max();
    if (binding.logical_bytes > maximum - binding.logical_base ||
        binding.registered_bytes > maximum - binding.registered_base)
        return {DestinationError::kOverflow, {}};
    if (binding.logical_base < binding.registered_base ||
        binding.logical_base - binding.registered_base >
            binding.registered_bytes ||
        binding.logical_bytes >
            binding.registered_bytes -
                (binding.logical_base - binding.registered_base))
        return {DestinationError::kInvalidArgument, {}};

    // Declare entries before the lock so an allocation failure releases the
    // external registration lifetime after unlocking.
    auto entry = std::make_shared<Entry>();
    entry->binding = binding;
    entry->lifetime = std::move(lifetime);
    std::lock_guard lock(state_->mutex);
    auto free = state_->slots.end();
    const std::pair<uint64_t, uint64_t> interval{
        binding.logical_base, binding.logical_base + binding.logical_bytes};
    for (auto slot = state_->slots.begin(); slot != state_->slots.end();
         ++slot) {
        if (!slot->entry) {
            if (free == state_->slots.end() && slot->generation != maximum)
                free = slot;
        } else {
            const auto& existing = slot->entry->binding;
            if (Overlap(interval,
                        {existing.logical_base,
                         existing.logical_base + existing.logical_bytes}))
                return {DestinationError::kOverlap, {}};
        }
    }
    if (free == state_->slots.end()) return {DestinationError::kLimit, {}};
    entry->region = {state_->requester,
                     static_cast<uint64_t>(free - state_->slots.begin() + 1),
                     free->generation + 1, credential};
    free->generation = entry->region.generation;
    free->entry = std::move(entry);
    return {DestinationError::kOk, free->entry->region};
}

DestinationRegistry::AcquireResult DestinationRegistry::Acquire(
    const AttemptIdentity& identity,
    const std::vector<RegisteredDestination>& destinations) {
    if (!ValidPeer(identity.owner) || !identity.request_id ||
        !identity.attempt_id)
        return {DestinationError::kInvalidArgument, {}};
    auto record = std::make_shared<Record>();
    record->identity = identity;
    record->fragments = destinations.size();
    std::lock_guard lock(state_->mutex);
    if (state_->records.contains(identity))
        return {DestinationError::kDuplicate, {}};
    auto peer = state_->replay_floors.find(identity.owner);
    if (peer != state_->replay_floors.end() &&
        identity.request_id <= peer->second)
        return {DestinationError::kStaleRequest, {}};
    if (state_->records.size() == state_->limits.max_tracked_attempts ||
        destinations.size() >
            state_->limits.max_active_fragments - state_->active_fragments ||
        (peer == state_->replay_floors.end() &&
         state_->replay_floors.size() == state_->limits.max_owner_sessions))
        return {DestinationError::kLimit, {}};
    std::set<uint64_t> seen_regions;
    for (const auto& destination : destinations) {
        std::shared_ptr<Entry> entry;
        auto error = state_->Find(destination.region, entry);
        if (error != DestinationError::kOk) return {error, {}};
        if (entry->closing) return {DestinationError::kClosing, {}};
        if (destination.offset > entry->binding.logical_bytes ||
            destination.length >
                entry->binding.logical_bytes - destination.offset)
            return {DestinationError::kInvalidArgument, {}};
        if (destination.length > state_->limits.max_active_bytes -
                                     state_->active_bytes - record->bytes)
            return {DestinationError::kLimit, {}};
        record->bytes += destination.length;
        const auto address = entry->binding.logical_base + destination.offset;
        if (destination.length)
            record->intervals.emplace_back(address,
                                           address + destination.length);
        record->views.push_back({state_->endpoint, address, destination.length,
                                 entry->binding.memory});
        if (seen_regions.insert(destination.region.region_id).second)
            record->entries.push_back(std::move(entry));
    }
    std::sort(record->intervals.begin(), record->intervals.end());
    for (size_t i = 1; i < record->intervals.size(); ++i)
        if (Overlap(record->intervals[i - 1], record->intervals[i]))
            return {DestinationError::kOverlap, {}};
    std::vector<std::pair<uint64_t, uint64_t>> reserved;
    reserved.reserve(state_->active_fragments);
    for (const auto& [key, existing] : state_->records) {
        (void)key;
        reserved.insert(reserved.end(), existing->intervals.begin(),
                        existing->intervals.end());
    }
    std::sort(reserved.begin(), reserved.end());
    size_t previous = 0;
    for (const auto& interval : record->intervals) {
        while (previous < reserved.size() &&
               reserved[previous].second <= interval.first)
            ++previous;
        if (previous < reserved.size() && Overlap(interval, reserved[previous]))
            return {DestinationError::kOverlap, {}};
    }
    // All validation and allocations precede reference/credit changes. Roll
    // back a newly inserted owner session if map insertion throws.
    auto [owner_it, new_owner] =
        state_->replay_floors.try_emplace(identity.owner, 0);
    try {
        state_->records.emplace(identity, record);
    } catch (...) {
        if (new_owner) state_->replay_floors.erase(owner_it);
        throw;
    }
    for (auto& entry : record->entries) ++entry->references;
    ++state_->active_attempts;
    state_->active_bytes += record->bytes;
    state_->active_fragments += record->fragments;
    return {DestinationError::kOk, Attempt(state_, std::move(record))};
}

DestinationError DestinationRegistry::BeginRevoke(
    const DestinationRegion& reference) {
    std::lock_guard lock(state_->mutex);
    std::shared_ptr<Entry> entry;
    auto error = state_->Find(reference, entry);
    if (error != DestinationError::kOk) return error;
    entry->closing = true;
    return DestinationError::kOk;
}
DestinationError DestinationRegistry::Retire(
    const DestinationRegion& reference) {
    std::shared_ptr<Entry> entry;
    std::shared_ptr<void> lifetime;
    {
        std::lock_guard lock(state_->mutex);
        auto error = state_->Find(reference, entry);
        if (error != DestinationError::kOk) return error;
        if (!entry->closing) return DestinationError::kClosing;
        if (entry->references || entry->retiring)
            return DestinationError::kBusy;
        entry->retiring = true;
        lifetime = std::move(entry->lifetime);
    }
    // The slot remains reserved during callback destruction, preventing a
    // concurrent registration from reusing the address during deregistration.
    lifetime.reset();
    {
        std::lock_guard lock(state_->mutex);
        state_->slots[reference.region_id - 1].entry.reset();
    }
    return DestinationError::kOk;
}
DestinationError DestinationRegistry::Complete(const DestinationDrainAck& ack) {
    std::lock_guard lock(state_->mutex);
    if (ack.requester != state_->requester)
        return DestinationError::kWrongAttempt;
    auto record = state_->records.find(ack.attempt);
    if (record == state_->records.end()) return DestinationError::kNotFound;
    return state_->Finish(*record->second, ack);
}
DestinationAttemptSnapshot DestinationRegistry::Inspect(
    const AttemptIdentity& key) const {
    std::lock_guard lock(state_->mutex);
    auto record = state_->records.find(key);
    return record == state_->records.end() ? DestinationAttemptSnapshot{}
                                           : state_->Snapshot(*record->second);
}
DestinationError DestinationRegistry::ForgetRequest(PeerIdentity owner,
                                                    uint64_t request_id) {
    std::lock_guard lock(state_->mutex);
    bool found = false;
    for (const auto& [key, record] : state_->records) {
        if (key.owner != owner || key.request_id != request_id) continue;
        found = true;
        if (record->stage != DestinationAttemptState::kDrained)
            return DestinationError::kBusy;
    }
    if (!found) return DestinationError::kNotFound;
    auto& floor = state_->replay_floors.at(owner);
    floor = std::max(floor, request_id);
    for (auto it = state_->records.begin(); it != state_->records.end();) {
        if (it->first.owner == owner && it->first.request_id == request_id)
            it = state_->records.erase(it);
        else
            ++it;
    }
    return DestinationError::kOk;
}
DestinationRegistryStats DestinationRegistry::stats() const {
    std::lock_guard lock(state_->mutex);
    DestinationRegistryStats result;
    for (const auto& slot : state_->slots) result.regions += bool(slot.entry);
    result.tracked_attempts = state_->records.size();
    result.active_attempts = state_->active_attempts;
    result.active_fragments = state_->active_fragments;
    result.active_bytes = state_->active_bytes;
    for (const auto& [key, record] : state_->records) {
        (void)key;
        result.quarantined_attempts +=
            record->stage == DestinationAttemptState::kQuarantined;
        result.fragment_array_bytes +=
            record->views.capacity() * sizeof(ResolvedDestination) +
            record->entries.capacity() * sizeof(std::shared_ptr<Entry>) +
            record->intervals.capacity() *
                sizeof(std::pair<uint64_t, uint64_t>);
    }
    return result;
}
bool DestinationRegistry::HasPendingWrite(uint64_t address,
                                          uint64_t bytes) const {
    if (bytes > UINT64_MAX - address) return true;
    std::lock_guard lock(state_->mutex);
    for (const auto& [identity, record] : state_->records) {
        (void)identity;
        if (record->stage == DestinationAttemptState::kDrained) continue;
        for (auto interval : record->intervals)
            if (Overlap({address, address + bytes}, interval)) return true;
    }
    return false;
}

DestinationRegistry::Attempt::Attempt(std::shared_ptr<State> state,
                                      std::shared_ptr<Record> record)
    : state_(std::move(state)), record_(std::move(record)) {}
DestinationRegistry::Attempt::Attempt(Attempt&&) noexcept = default;
DestinationRegistry::Attempt& DestinationRegistry::Attempt::operator=(
    Attempt&& other) noexcept {
    if (this != &other) {
        Abandon();
        state_ = std::move(other.state_);
        record_ = std::move(other.record_);
    }
    return *this;
}
DestinationRegistry::Attempt::~Attempt() { Abandon(); }
void DestinationRegistry::Attempt::Abandon() noexcept {
    if (!state_ || !record_) return;
    std::lock_guard lock(state_->mutex);
    if (record_->stage == DestinationAttemptState::kPrepared)
        state_->Release(*record_, false, 0);
    else if (record_->stage == DestinationAttemptState::kExposed)
        record_->stage = DestinationAttemptState::kQuarantined;
}
DestinationError DestinationRegistry::Attempt::Publish(
    std::vector<ResolvedDestination>& destinations) {
    destinations.clear();
    if (!state_ || !record_) return DestinationError::kInvalidArgument;
    std::lock_guard lock(state_->mutex);
    if (record_->stage != DestinationAttemptState::kPrepared)
        return DestinationError::kDuplicate;
    auto copy = record_->views;
    state_->drain_hold = state_;
    ++state_->exposed;
    record_->stage = DestinationAttemptState::kExposed;
    destinations.swap(copy);
    return DestinationError::kOk;
}
DestinationError DestinationRegistry::Attempt::Timeout() {
    if (!state_ || !record_) return DestinationError::kInvalidArgument;
    std::lock_guard lock(state_->mutex);
    if (record_->stage == DestinationAttemptState::kDrained)
        return DestinationError::kDuplicate;
    if (record_->stage == DestinationAttemptState::kPrepared)
        state_->Release(*record_, false, 0);
    else
        record_->stage = DestinationAttemptState::kQuarantined;
    return DestinationError::kOk;
}
DestinationError DestinationRegistry::Attempt::Complete(
    const DestinationDrainAck& ack) {
    if (!state_ || !record_) return DestinationError::kInvalidArgument;
    std::lock_guard lock(state_->mutex);
    return state_->Finish(*record_, ack);
}
DestinationAttemptSnapshot DestinationRegistry::Attempt::snapshot() const {
    if (!state_ || !record_) return {};
    std::lock_guard lock(state_->mutex);
    return state_->Snapshot(*record_);
}

}  // namespace mooncake::ssd_offload

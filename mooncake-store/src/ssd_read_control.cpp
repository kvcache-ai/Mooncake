// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_control.h"
#include "random.h"

#include <algorithm>
#include <map>
#include <mutex>
#include <stdexcept>
#include <tuple>

namespace mooncake::ssd_offload {
namespace {
auto PeerKey(PeerIdentity p) {
    return std::tuple(p.client_id_high, p.client_id_low, p.boot_epoch);
}
auto AttemptKey(AttemptIdentity a) {
    return std::tuple(PeerKey(a.owner), a.request_id, a.attempt_id);
}
auto GrantKey(const SsdGrantReferenceV1& g) {
    return std::tuple(PeerKey(g.requester), AttemptKey(g.attempt));
}
bool ValidPeer(PeerIdentity p) {
    return (p.client_id_high || p.client_id_low) && p.boot_epoch;
}
bool ValidLimits(SsdControlLimits l) {
    return l.max_records && l.max_records <= 65536 && l.max_sessions &&
           l.max_sessions <= 65536 && l.max_metadata_bytes &&
           l.max_active_payload_bytes;
}
bool SameCredential(WriteCredential a, WriteCredential b) {
    return ((a.high ^ b.high) | (a.low ^ b.low)) == 0;
}
// Conservative retained metadata credits, including terminal results and,
// for grants, the registry's bounded 4096-byte endpoint in each cached view.
// Does not claim to measure allocator or caller-held small handle overhead.
bool Charge(const SsdReadRequestV1& request, bool grant, uint64_t cap,
            uint64_t& bytes, uint64_t& payload) {
    bytes = 0;
    payload = 0;
    auto add = [&](uint64_t count, uint64_t size) {
        if (count > (cap - bytes) / size) return false;
        bytes += count * size;
        return true;
    };
    if (!add(1, 512) || !add(request.tenant_id.size(), 1) ||
        !add(request.objects.size(),
             sizeof(SsdObjectReadV1) + sizeof(SsdObjectReadResultV1)))
        return false;
    for (const auto& object : request.objects) {
        if (!add(object.key.size(), 1) ||
            !add(object.ranges.size(), sizeof(SsdReadRangeV1)))
            return false;
        for (const auto& range : object.ranges) {
            if (!add(range.destinations.size(),
                     sizeof(RegisteredDestination) +
                         (grant ? sizeof(ResolvedDestination) + 4096 : 0)))
                return false;
            for (const auto& d : range.destinations) payload += d.length;
        }
    }
    return true;
}
DestinationError Match(const SsdGrantReferenceV1& reference,
                       const SsdReadRequestV1& request) {
    if (reference.requester != request.requester ||
        reference.attempt != request.attempt)
        return DestinationError::kWrongAttempt;
    if (reference.credential == WriteCredential{})
        return DestinationError::kUnauthorized;
    return DestinationError::kOk;
}
DestinationDrainAck Ack(const SsdReadResultV1& result, bool resolved) {
    uint64_t bytes = 0;
    bool all_ok = true, all_rejected = true, any_cancelled = false;
    for (const auto& object : result.objects) {
        bytes += object.valid_bytes;
        all_ok &= object.outcome == AttemptOutcome::kSucceeded;
        all_rejected &= object.outcome == AttemptOutcome::kRejected;
        any_cancelled |= object.outcome == AttemptOutcome::kCancelled;
    }
    const auto outcome = !resolved || all_rejected ? AttemptOutcome::kRejected
                         : all_ok                  ? AttemptOutcome::kSucceeded
                         : any_cancelled           ? AttemptOutcome::kCancelled
                                                   : AttemptOutcome::kFailed;
    return {result.attempt, outcome, bytes, result.dma_drained,
            result.requester};
}
}  // namespace

struct SsdRequesterGrants::Impl {
    struct Record {
        SsdReadRequestV1 request;
        SsdGrantReferenceV1 reference;
        DestinationRegistry::Attempt attempt;
        std::vector<ResolvedDestination> views;
        std::optional<SsdReadResultV1> result;
        uint64_t metadata{0}, payload{0};
        bool resolved{false}, timed_out{false}, active{true};
    };
    Impl(PeerIdentity p, std::shared_ptr<DestinationRegistration> registration,
         SsdControlLimits limits, SsdReadProtocolLimits protocol)
        : requester(p),
          registration(std::move(registration)),
          limits(limits),
          protocol(protocol) {}
    DestinationError Find(const SsdGrantReferenceV1& ref,
                          std::shared_ptr<Record>& record) const {
        if (ref.requester != requester) return DestinationError::kWrongAttempt;
        auto it = records.find(AttemptKey(ref.attempt));
        if (it == records.end()) return DestinationError::kNotFound;
        if (!SameCredential(ref.credential, it->second->reference.credential))
            return DestinationError::kUnauthorized;
        record = it->second;
        return DestinationError::kOk;
    }
    void ReleaseActive(Record& r) {
        if (r.active) {
            active -= r.payload;
            r.active = false;
        }
    }
    mutable std::mutex mutex;
    PeerIdentity requester;
    std::shared_ptr<DestinationRegistration> registration;
    SsdControlLimits limits;
    SsdReadProtocolLimits protocol;
    std::map<decltype(AttemptKey({})), std::shared_ptr<Record>> records;
    std::map<decltype(PeerKey({})), bool> peers;
    uint64_t metadata{0}, active{0};
    bool closing{false};
};
SsdRequesterGrants::SsdRequesterGrants(
    PeerIdentity requester,
    std::shared_ptr<DestinationRegistration> registration,
    SsdControlLimits limits, SsdReadProtocolLimits protocol) {
    if (!ValidPeer(requester) || !registration || !ValidLimits(limits))
        throw std::invalid_argument("Invalid SSD grant registry configuration");
    impl_ = std::make_unique<Impl>(requester, std::move(registration), limits,
                                   protocol);
}
SsdRequesterGrants::~SsdRequesterGrants() { Close(); }
SsdGrantPreparation SsdRequesterGrants::Prepare(
    const SsdReadRequestV1& request) {
    auto error = ValidateSsdReadRequestV1(request, request.attempt.owner,
                                          impl_->protocol);
    if (error != DestinationError::kOk) return {error, {}};
    if (request.requester != impl_->requester)
        return {DestinationError::kWrongAttempt, {}};
    uint64_t metadata, payload;
    if (!Charge(request, true, impl_->limits.max_metadata_bytes, metadata,
                payload))
        return {DestinationError::kLimit, {}};
    auto record = std::make_shared<Impl::Record>();
    record->request = request;
    record->metadata = metadata;
    record->payload = payload;
    record->reference = {request.requester, request.attempt, {}};
    if (!secureRandomBytes(std::as_writable_bytes(
            std::span(&record->reference.credential, 1))) ||
        record->reference.credential == WriteCredential{})
        return {DestinationError::kNotSupported, {}};
    std::vector<RegisteredDestination> destinations;
    for (const auto& object : request.objects)
        for (const auto& range : object.ranges)
            destinations.insert(destinations.end(), range.destinations.begin(),
                                range.destinations.end());
    std::lock_guard lock(impl_->mutex);
    if (impl_->closing) return {DestinationError::kClosing, {}};
    if (impl_->records.contains(AttemptKey(request.attempt)))
        return {DestinationError::kDuplicate, {}};
    if (impl_->records.size() == impl_->limits.max_records ||
        metadata > impl_->limits.max_metadata_bytes - impl_->metadata ||
        payload > impl_->limits.max_active_payload_bytes - impl_->active ||
        (!impl_->peers.contains(PeerKey(request.attempt.owner)) &&
         impl_->peers.size() == impl_->limits.max_sessions))
        return {DestinationError::kLimit, {}};
    const auto [peer, inserted] =
        impl_->peers.emplace(PeerKey(request.attempt.owner), true);
    try {
        impl_->records.emplace(AttemptKey(request.attempt), record);
        auto acquired =
            impl_->registration->Acquire(request.attempt, destinations);
        if (acquired.error != DestinationError::kOk) {
            impl_->records.erase(AttemptKey(request.attempt));
            if (inserted) impl_->peers.erase(peer);
            return {acquired.error, {}};
        }
        record->attempt = std::move(acquired.attempt);
    } catch (...) {
        impl_->records.erase(AttemptKey(request.attempt));
        if (inserted) impl_->peers.erase(peer);
        throw;
    }
    impl_->metadata += metadata;
    impl_->active += payload;
    return {DestinationError::kOk, record->reference};
}
SsdGrantResolution SsdRequesterGrants::Resolve(
    const SsdGrantReferenceV1& ref, const SsdReadRequestV1& request) {
    std::lock_guard lock(impl_->mutex);
    std::shared_ptr<Impl::Record> record;
    auto error = impl_->Find(ref, record);
    if (error != DestinationError::kOk) return {error, {}};
    if (request != record->request)
        return {DestinationError::kUnauthorized, {}};
    if (impl_->closing || record->timed_out || record->result)
        return {DestinationError::kClosing, {}};
    if (!record->resolved) {
        error = record->attempt.Publish(record->views);
        if (error != DestinationError::kOk) return {error, {}};
        record->resolved = true;
    }
    return {DestinationError::kOk, record->views};
}
DestinationError SsdRequesterGrants::Timeout(const SsdGrantReferenceV1& ref) {
    std::lock_guard lock(impl_->mutex);
    std::shared_ptr<Impl::Record> record;
    auto error = impl_->Find(ref, record);
    if (error != DestinationError::kOk) return error;
    error = record->attempt.Timeout();
    if (error != DestinationError::kOk) return error;
    record->timed_out = true;
    if (record->attempt.snapshot().state == DestinationAttemptState::kDrained)
        impl_->ReleaseActive(*record);
    return error;
}
DestinationError SsdRequesterGrants::Complete(const SsdGrantReferenceV1& ref,
                                              const SsdReadResultV1& result) {
    std::lock_guard lock(impl_->mutex);
    std::shared_ptr<Impl::Record> record;
    auto error = impl_->Find(ref, record);
    if (error != DestinationError::kOk) return error;
    error = ValidateSsdReadResultV1(record->request, result, impl_->protocol);
    if (error != DestinationError::kOk) return error;
    if (record->result)
        return *record->result == result ? DestinationError::kDuplicate
                                         : DestinationError::kWrongAttempt;
    if (!record->resolved &&
        std::any_of(result.objects.begin(), result.objects.end(),
                    [](const auto& o) { return o.valid_bytes != 0; }))
        return DestinationError::kInvalidArgument;
    auto copy = result;
    error = record->attempt.Complete(Ack(result, record->resolved));
    if (error == DestinationError::kDuplicate && !record->resolved &&
        record->timed_out &&
        record->attempt.snapshot().state == DestinationAttemptState::kDrained)
        error = DestinationError::kOk;
    if (error != DestinationError::kOk) return error;
    record->result = std::move(copy);
    impl_->ReleaseActive(*record);
    // Addresses are no longer usable; release the cached view storage.
    std::vector<ResolvedDestination>().swap(record->views);
    return DestinationError::kOk;
}
DestinationAttemptSnapshot SsdRequesterGrants::Inspect(
    const SsdGrantReferenceV1& ref) const {
    std::lock_guard lock(impl_->mutex);
    std::shared_ptr<Impl::Record> record;
    auto error = impl_->Find(ref, record);
    return error == DestinationError::kOk ? record->attempt.snapshot()
                                          : DestinationAttemptSnapshot{error};
}
DestinationError SsdRequesterGrants::Forget(PeerIdentity owner, uint64_t id) {
    std::lock_guard lock(impl_->mutex);
    const auto error = impl_->registration->ForgetRequest(owner, id);
    if (error != DestinationError::kOk) return error;
    for (auto it = impl_->records.begin(); it != impl_->records.end();) {
        auto& record = *it->second;
        if (record.reference.attempt.owner == owner &&
            record.reference.attempt.request_id == id) {
            impl_->ReleaseActive(record);
            impl_->metadata -= record.metadata;
            it = impl_->records.erase(it);
        } else
            ++it;
    }
    return DestinationError::kOk;
}
void SsdRequesterGrants::Close() {
    std::lock_guard lock(impl_->mutex);
    impl_->closing = true;
    for (auto& [key, record] : impl_->records) {
        (void)key;
        if (record->attempt.Timeout() == DestinationError::kOk)
            record->timed_out = true;
        if (record->attempt.snapshot().state ==
            DestinationAttemptState::kDrained)
            impl_->ReleaseActive(*record);
    }
}
SsdControlStats SsdRequesterGrants::stats() const {
    std::lock_guard lock(impl_->mutex);
    return {impl_->records.size(), impl_->metadata, impl_->active};
}

struct SsdOwnerRequestLedger::Record {
    SsdReadRequestV1 request;
    SsdGrantReferenceV1 reference;
    SsdOwnerRequestState state{SsdOwnerRequestState::kAdmitted};
    std::optional<SsdReadResultV1> result;
    uint64_t metadata{0}, payload{0};
    bool forgotten{false};
    std::string binding;
    std::chrono::steady_clock::time_point deadline;
};
struct SsdOwnerRequestLedger::State {
    State(PeerIdentity owner, SsdControlLimits limits,
          SsdReadProtocolLimits protocol, bool bounded_cache)
        : owner(owner),
          limits(limits),
          protocol(protocol),
          bounded_cache(bounded_cache) {}
    DestinationError Find(const SsdGrantReferenceV1& ref,
                          std::shared_ptr<Record>& record) const {
        if (ref.attempt.owner != owner) return DestinationError::kWrongAttempt;
        auto it = records.find(GrantKey(ref));
        if (it == records.end()) return DestinationError::kNotFound;
        if (!SameCredential(ref.credential, it->second->reference.credential))
            return DestinationError::kUnauthorized;
        record = it->second;
        return DestinationError::kOk;
    }
    mutable std::mutex mutex;
    PeerIdentity owner;
    SsdControlLimits limits;
    SsdReadProtocolLimits protocol;
    bool bounded_cache;
    std::map<decltype(GrantKey({})), std::shared_ptr<Record>> records;
    std::map<decltype(PeerKey({})), uint64_t> replay_floors;
    uint64_t metadata{0}, active{0};
    bool closing{false};
};
SsdOwnerRequestLedger::SsdOwnerRequestLedger(PeerIdentity owner,
                                             SsdControlLimits limits,
                                             SsdReadProtocolLimits protocol,
                                             bool bounded_cache) {
    if (!ValidPeer(owner) || !ValidLimits(limits))
        throw std::invalid_argument("Invalid SSD owner ledger configuration");
    state_ = std::make_shared<State>(owner, limits, protocol, bounded_cache);
}
SsdOwnerRequestLedger::~SsdOwnerRequestLedger() { Close(); }
SsdOwnerRequestLedger::Admission SsdOwnerRequestLedger::Admit(
    const SsdReadRequestV1& request, const SsdGrantReferenceV1& ref,
    std::chrono::steady_clock::time_point received_at,
    std::string_view binding) {
    if (received_at < std::chrono::steady_clock::time_point{} ||
        received_at > std::chrono::steady_clock::now())
        return {DestinationError::kInvalidArgument, {}};
    auto error =
        ValidateSsdReadRequestV1(request, state_->owner, state_->protocol);
    if (error != DestinationError::kOk) return {error, {}};
    error = Match(ref, request);
    if (error != DestinationError::kOk) return {error, {}};
    uint64_t metadata, payload;
    if (!Charge(request, false, state_->limits.max_metadata_bytes, metadata,
                payload))
        return {DestinationError::kLimit, {}};
    if (binding.size() > state_->limits.max_metadata_bytes - metadata)
        return {DestinationError::kLimit, {}};
    metadata += binding.size();
    auto record = std::make_shared<Record>();
    record->request = request;
    record->reference = ref;
    record->binding = binding;
    record->metadata = metadata;
    record->payload = payload;
    std::lock_guard lock(state_->mutex);
    if (state_->closing) return {DestinationError::kClosing, {}};
    if (auto it = state_->records.find(GrantKey(ref));
        it != state_->records.end())
        return {it->second->request == request &&
                        it->second->binding == binding &&
                        SameCredential(ref.credential,
                                       it->second->reference.credential)
                    ? DestinationError::kDuplicate
                    : DestinationError::kUnauthorized,
                {}};
    auto peer = state_->replay_floors.find(PeerKey(ref.requester));
    if (peer != state_->replay_floors.end() &&
        ref.attempt.request_id <= peer->second)
        return {DestinationError::kStaleRequest, {}};
    // Terminal cache eviction is a metadata operation, never an I/O timeout.
    // Advance a persistent replay floor before forgetting the cached result.
    while (state_->bounded_cache &&
           (state_->records.size() >= state_->limits.max_records ||
            metadata > state_->limits.max_metadata_bytes - state_->metadata)) {
        auto retired = std::find_if(
            state_->records.begin(), state_->records.end(),
            [](const auto& item) {
                return item.second->state == SsdOwnerRequestState::kDrained;
            });
        if (retired == state_->records.end()) break;
        auto& old = *retired->second;
        auto& floor =
            state_->replay_floors.at(PeerKey(old.reference.requester));
        floor = std::max(floor, old.reference.attempt.request_id);
        state_->metadata -= old.metadata;
        old.forgotten = true;
        old.request = {};
        old.result.reset();
        old.binding.clear();
        state_->records.erase(retired);
    }
    // Eviction may have advanced this requester's floor since the first check.
    peer = state_->replay_floors.find(PeerKey(ref.requester));
    if (peer != state_->replay_floors.end() &&
        ref.attempt.request_id <= peer->second)
        return {DestinationError::kStaleRequest, {}};
    if (state_->records.size() == state_->limits.max_records ||
        metadata > state_->limits.max_metadata_bytes - state_->metadata ||
        payload > state_->limits.max_active_payload_bytes - state_->active ||
        (peer == state_->replay_floors.end() &&
         state_->replay_floors.size() == state_->limits.max_sessions))
        return {DestinationError::kLimit, {}};
    const auto [session, inserted] =
        state_->replay_floors.emplace(PeerKey(ref.requester), 0);
    try {
        state_->records.emplace(GrantKey(ref), record);
    } catch (...) {
        if (inserted) state_->replay_floors.erase(session);
        throw;
    }
    const auto now = received_at;
    const auto maximum = std::chrono::steady_clock::time_point::max();
    const auto capacity =
        std::chrono::duration_cast<std::chrono::milliseconds>(maximum - now)
            .count();
    record->deadline =
        request.relative_timeout_ms >= uint64_t(capacity)
            ? maximum
            : now + std::chrono::milliseconds(request.relative_timeout_ms);
    state_->metadata += metadata;
    state_->active += payload;
    return {DestinationError::kOk, Operation(state_, record)};
}
SsdOwnerRequestSnapshot SsdOwnerRequestLedger::Query(
    const SsdGrantReferenceV1& ref) const {
    std::lock_guard lock(state_->mutex);
    std::shared_ptr<Record> record;
    const auto error = state_->Find(ref, record);
    return error == DestinationError::kOk
               ? SsdOwnerRequestSnapshot{error, record->state, record->result}
               : SsdOwnerRequestSnapshot{error};
}
SsdOwnerRequestSnapshot SsdOwnerRequestLedger::Recover(
    const SsdReadRequestV1& request, const SsdGrantReferenceV1& ref,
    std::string_view binding) {
    auto error =
        ValidateSsdReadRequestV1(request, state_->owner, state_->protocol);
    if (error != DestinationError::kOk) return {error};
    error = Match(ref, request);
    if (error != DestinationError::kOk) return {error};
    // Prepare the shaped zero-byte response before changing any replay state.
    SsdReadResultV1 cancelled{1, request.attempt, true, {}, request.requester};
    for (size_t i = 0; i < request.objects.size(); ++i)
        cancelled.objects.push_back({i, AttemptOutcome::kCancelled, 0, {}});
    std::lock_guard lock(state_->mutex);
    if (!state_->bounded_cache) return {DestinationError::kNotSupported};
    std::shared_ptr<Record> record;
    error = state_->Find(ref, record);
    if (error == DestinationError::kOk) {
        if (record->request != request || record->binding != binding)
            return {DestinationError::kUnauthorized};
        return {DestinationError::kOk, record->state, record->result};
    }
    if (error != DestinationError::kNotFound) return {error};
    auto peer = state_->replay_floors.find(PeerKey(ref.requester));
    if (peer == state_->replay_floors.end() &&
        state_->replay_floors.size() == state_->limits.max_sessions)
        return {DestinationError::kLimit};
    auto [session, inserted] =
        state_->replay_floors.emplace(PeerKey(ref.requester), 0);
    (void)inserted;
    session->second = std::max(session->second, ref.attempt.request_id);
    // There is no accepted task for this attempt, and any delayed admission is
    // now rejected under the same mutex. An evicted terminal was already
    // drained.
    return {DestinationError::kOk, SsdOwnerRequestState::kDrained,
            std::move(cancelled)};
}
DestinationError SsdOwnerRequestLedger::Forget(const SsdGrantReferenceV1& ref) {
    std::lock_guard lock(state_->mutex);
    std::shared_ptr<Record> record;
    const auto error = state_->Find(ref, record);
    if (error != DestinationError::kOk) return error;
    for (const auto& [key, r] : state_->records) {
        (void)key;
        if (r->reference.requester == ref.requester &&
            r->reference.attempt.request_id == ref.attempt.request_id &&
            r->state != SsdOwnerRequestState::kDrained)
            return DestinationError::kBusy;
    }
    auto& floor = state_->replay_floors.at(PeerKey(ref.requester));
    floor = std::max(floor, ref.attempt.request_id);
    for (auto it = state_->records.begin(); it != state_->records.end();) {
        auto& r = *it->second;
        if (r.reference.requester == ref.requester &&
            r.reference.attempt.request_id == ref.attempt.request_id) {
            state_->metadata -= r.metadata;
            r.forgotten = true;
            r.request = {};
            r.result.reset();
            it = state_->records.erase(it);
        } else
            ++it;
    }
    return DestinationError::kOk;
}
void SsdOwnerRequestLedger::Close() {
    std::lock_guard lock(state_->mutex);
    state_->closing = true;
}
SsdControlStats SsdOwnerRequestLedger::stats() const {
    std::lock_guard lock(state_->mutex);
    return {state_->records.size(), state_->metadata, state_->active};
}
SsdOwnerRequestLedger::Operation::Operation(std::shared_ptr<State> state,
                                            std::shared_ptr<Record> record)
    : state_(std::move(state)), record_(std::move(record)) {}
SsdOwnerRequestLedger::Operation::Operation(Operation&&) noexcept = default;
SsdOwnerRequestLedger::Operation& SsdOwnerRequestLedger::Operation::operator=(
    Operation&& other) noexcept {
    if (this != &other) {
        Abandon();
        state_ = std::move(other.state_);
        record_ = std::move(other.record_);
    }
    return *this;
}
SsdOwnerRequestLedger::Operation::~Operation() { Abandon(); }
void SsdOwnerRequestLedger::Operation::Abandon() noexcept {
    if (!state_ || !record_) return;
    std::lock_guard lock(state_->mutex);
    if (!record_->forgotten && record_->state != SsdOwnerRequestState::kDrained)
        record_->state = SsdOwnerRequestState::kQuarantined;
}
DestinationError SsdOwnerRequestLedger::Operation::Start() {
    if (!state_ || !record_) return DestinationError::kInvalidArgument;
    std::lock_guard lock(state_->mutex);
    if (record_->forgotten) return DestinationError::kStaleRequest;
    if (record_->state != SsdOwnerRequestState::kAdmitted)
        return DestinationError::kDuplicate;
    if (std::chrono::steady_clock::now() >= record_->deadline)
        return DestinationError::kClosing;
    record_->state = SsdOwnerRequestState::kRunning;
    return DestinationError::kOk;
}
DestinationError SsdOwnerRequestLedger::Operation::Complete(
    const SsdReadResultV1& result) {
    if (!state_ || !record_) return DestinationError::kInvalidArgument;
    std::lock_guard lock(state_->mutex);
    if (record_->forgotten) return DestinationError::kStaleRequest;
    const auto error =
        ValidateSsdReadResultV1(record_->request, result, state_->protocol);
    if (error != DestinationError::kOk) return error;
    if (record_->result)
        return *record_->result == result ? DestinationError::kDuplicate
                                          : DestinationError::kWrongAttempt;
    if (record_->state == SsdOwnerRequestState::kAdmitted &&
        std::any_of(result.objects.begin(), result.objects.end(),
                    [](const auto& o) {
                        return o.valid_bytes ||
                               (o.outcome != AttemptOutcome::kRejected &&
                                o.outcome != AttemptOutcome::kCancelled);
                    }))
        return DestinationError::kInvalidArgument;
    auto copy = result;
    record_->result = std::move(copy);
    record_->state = SsdOwnerRequestState::kDrained;
    state_->active -= record_->payload;
    return DestinationError::kOk;
}
std::chrono::milliseconds SsdOwnerRequestLedger::Operation::RemainingTimeout()
    const {
    if (!state_ || !record_) return std::chrono::milliseconds::zero();
    std::lock_guard lock(state_->mutex);
    const auto now = std::chrono::steady_clock::now();
    if (record_->forgotten ||
        record_->state == SsdOwnerRequestState::kDrained ||
        now >= record_->deadline)
        return std::chrono::milliseconds::zero();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        record_->deadline - now);
}
}  // namespace mooncake::ssd_offload

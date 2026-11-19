// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_fixed_l2_requester.h"
#include "random.h"
#include "tenant_id.h"
#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <tuple>

namespace mooncake::ssd_offload {
namespace {
auto Key(AttemptIdentity a) {
    return std::tuple(a.owner.client_id_high, a.owner.client_id_low,
                      a.owner.boot_epoch, a.request_id, a.attempt_id);
}
bool Valid(PeerIdentity p) {
    return (p.client_id_high || p.client_id_low) && p.boot_epoch;
}
DestinationDrainAck Ack(const SsdReadResultV1& result) {
    uint64_t bytes = 0;
    bool ok = true, rejected = true;
    for (const auto& object : result.objects) {
        bytes += object.valid_bytes;
        ok &= object.outcome == AttemptOutcome::kSucceeded;
        rejected &= object.outcome == AttemptOutcome::kRejected;
    }
    return {result.attempt,
            ok         ? AttemptOutcome::kSucceeded
            : rejected ? AttemptOutcome::kRejected
                       : AttemptOutcome::kFailed,
            bytes, result.dma_drained, result.requester};
}
}  // namespace
struct SsdFixedL2Requester::State {
    struct Pending {
        std::mutex mutex;
        SsdControlMessage message;
        SsdPushPeerInfo route;
        DestinationRegistry::Attempt attempt;
        std::chrono::steady_clock::time_point deadline;
        bool timed_out{false};
        std::optional<SsdFixedL2ReadResult> terminal;
    };
    State(SsdFixedL2Peer self, std::string protocol,
          SsdRequesterControlCall call, DestinationRegistryLimits limits,
          SsdReadProtocolLimits wire)
        : self(std::move(self)),
          protocol(std::move(protocol)),
          call(std::move(call)),
          registry(this->self.identity, this->self.transport_endpoint, limits),
          wire(wire) {}
    SsdRequesterCallResult Call(Pending& pending, SsdControlCommand command,
                                std::chrono::milliseconds timeout) noexcept {
        try {
            auto message = pending.message;
            message.command = command;
            return call(pending.route.control_host, pending.route.control_port,
                        message, timeout);
        } catch (...) {
            return {};
        }
    }
    SsdFixedL2ReadResult Finish(const std::shared_ptr<Pending>& pending,
                                SsdRequesterCallResult call,
                                SsdControlCommand expected) {
        const auto id = pending->message.reference.attempt;
        auto fail = [&](DestinationError error) {
            pending->timed_out = true;
            pending->attempt.Timeout();
            return SsdFixedL2ReadResult{error, true, id, {}};
        };
        if (call.error != DestinationError::kOk) return fail(call.error);
        if (!call.reply) return fail(DestinationError::kInvalidArgument);
        if (call.reply->command != expected)
            return fail(DestinationError::kWrongAttempt);
        if (call.reply->error != DestinationError::kOk)
            return fail(call.reply->error);
        if (!call.reply->snapshot || !call.reply->snapshot->result)
            return fail(DestinationError::kNotDrained);
        if (call.reply->snapshot->error != DestinationError::kOk)
            return fail(call.reply->snapshot->error);
        if (call.reply->snapshot->state != SsdOwnerRequestState::kDrained)
            return fail(DestinationError::kNotDrained);
        auto result = *call.reply->snapshot->result;
        const auto valid =
            ValidateSsdReadResultV1(*pending->message.request, result, wire);
        if (valid != DestinationError::kOk) return fail(valid);
        if (closing || std::chrono::steady_clock::now() >= pending->deadline) {
            pending->timed_out = true;
            pending->attempt.Timeout();
        }
        const auto completed = pending->attempt.Complete(Ack(result));
        if (completed != DestinationError::kOk) return fail(completed);
        if (closing || std::chrono::steady_clock::now() >= pending->deadline)
            pending->timed_out = true;
        const bool publishable =
            pending->attempt.snapshot().publishable && !pending->timed_out;
        const bool allow_fallback =
            expected == SsdControlCommand::kReadInto && !pending->timed_out &&
            !closing && std::chrono::steady_clock::now() < pending->deadline &&
            std::all_of(
                result.objects.begin(), result.objects.end(),
                [](const auto& object) {
                    return object.outcome == AttemptOutcome::kRejected;
                });
        const auto forgotten = registry.ForgetRequest(id.owner, id.request_id);
        if (forgotten != DestinationError::kOk) return fail(forgotten);
        pending->terminal.emplace(SsdFixedL2ReadResult{
            publishable ? DestinationError::kOk : DestinationError::kClosing,
            true, id, std::move(result), allow_fallback});
        {
            std::lock_guard lock(mutex);
            pending_reads.erase(Key(id));
        }
        return *pending->terminal;
    }
    SsdFixedL2Peer self;
    std::string protocol;
    SsdRequesterControlCall call;
    DestinationRegistry registry;
    SsdReadProtocolLimits wire;
    mutable std::mutex mutex;
    std::map<decltype(Key({})), std::shared_ptr<Pending>> pending_reads;
    std::vector<DestinationRegion> regions;
    uint64_t next_id{1};
    std::atomic<bool> closing{false};
    std::shared_ptr<State> quarantine;
};
SsdFixedL2Requester::SsdFixedL2Requester(SsdFixedL2Peer self,
                                         std::string protocol,
                                         SsdRequesterControlCall call,
                                         DestinationRegistryLimits limits,
                                         SsdReadProtocolLimits wire) {
    if (!Valid(self.identity) || !TenantId(self.tenant_id).IsValid() ||
        self.credential == WriteCredential{} ||
        self.transport_endpoint.empty() || !call ||
        (protocol != "tcp" && protocol != "rdma"))
        throw std::invalid_argument("Invalid fixed L2 requester configuration");
    state_ = std::make_shared<State>(std::move(self), std::move(protocol),
                                     std::move(call), limits, wire);
}
SsdFixedL2Requester::~SsdFixedL2Requester() {
    try {
        if (Close() == DestinationError::kOk) return;
    } catch (...) {
    }
    state_->quarantine = state_;
}
DestinationRegistry::RegistrationResult SsdFixedL2Requester::Register(
    DestinationBinding binding, std::shared_ptr<void> mapping) {
    std::lock_guard lock(state_->mutex);
    if (state_->closing) return {DestinationError::kClosing, {}};
    if (binding.memory != DestinationMemory::kHost)
        return {DestinationError::kNotSupported, {}};
    WriteCredential credential;
    if (!secureRandomBytes(std::as_writable_bytes(std::span(&credential, 1))) ||
        credential == WriteCredential{})
        return {DestinationError::kNotSupported, {}};
    state_->regions.reserve(state_->regions.size() + 1);
    auto registered =
        state_->registry.Register(binding, credential, std::move(mapping));
    if (registered.error == DestinationError::kOk)
        state_->regions.push_back(registered.region);
    return registered;
}
SsdFixedL2ReadResult SsdFixedL2Requester::Read(
    const SsdPushPeerInfo& peer, std::vector<SsdObjectReadV1> objects,
    std::chrono::milliseconds timeout) {
    if (timeout.count() <= 0 || timeout > std::chrono::seconds(60)) return {};
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    auto pending = std::make_shared<State::Pending>();
    {
        std::lock_guard lock(state_->mutex);
        if (state_->closing) return {DestinationError::kClosing, false, {}, {}};
        if (peer.tenant_id != state_->self.tenant_id || !peer.control_port ||
            !CanUseSsdPushV1(peer.capability, peer.owner, state_->protocol))
            return {DestinationError::kNotSupported, false, {}, {}};
        if (state_->next_id == UINT64_MAX)
            return {DestinationError::kLimit, false, {}, {}};
        pending->route = peer;
        pending->deadline = deadline;
        SsdReadRequestV1 request{1,
                                 state_->self.identity,
                                 {peer.owner, state_->next_id++, 1},
                                 state_->self.tenant_id,
                                 uint64_t(timeout.count()),
                                 0,
                                 std::move(objects)};
        pending->message = {
            SsdControlCommand::kReadInto,
            {request.requester, request.attempt, state_->self.credential},
            std::move(request),
            {}};
        const auto valid = ValidateSsdReadRequestV1(*pending->message.request,
                                                    peer.owner, state_->wire);
        if (valid != DestinationError::kOk) return {valid, false, {}, {}};
        size_t fragments = 0;
        uint64_t bytes = 0;
        std::vector<RegisteredDestination> destinations;
        for (const auto& object : pending->message.request->objects)
            for (const auto& range : object.ranges)
                for (const auto& d : range.destinations) {
                    ++fragments;
                    bytes += d.length;
                    destinations.push_back(d);
                }
        if (pending->message.request->objects.size() >
                peer.capability.max_batch_objects ||
            fragments > peer.capability.max_fragments ||
            bytes > peer.capability.max_batch_bytes ||
            bytes > peer.capability.max_inflight_bytes)
            return {DestinationError::kLimit, false, {}, {}};
        auto acquired = state_->registry.Acquire(
            pending->message.reference.attempt, destinations);
        if (acquired.error != DestinationError::kOk)
            return {acquired.error, false, {}, {}};
        pending->attempt = std::move(acquired.attempt);
        const auto id = pending->message.reference.attempt;
        bool locally_exposed = false;
        auto abandon_unpublished = [&] {
            // No control callback has run, so no remote writer can know this
            // attempt. Local Publish alone does not submit a network request.
            if (locally_exposed)
                pending->attempt.Complete({id, AttemptOutcome::kRejected, 0,
                                           true, state_->self.identity});
            else
                pending->attempt.Timeout();
            state_->registry.ForgetRequest(id.owner, id.request_id);
            state_->pending_reads.erase(Key(id));
        };
        try {
            // Allocate the recovery record before exposing an address. Both
            // map insertion and Publish's view copy can fail before exposure.
            state_->pending_reads.emplace(Key(id), pending);
            const auto published =
                pending->attempt.Publish(pending->message.destinations);
            if (published != DestinationError::kOk) {
                abandon_unpublished();
                return {published, false, {}, {}};
            }
            locally_exposed = true;
            const auto encoded =
                EncodeSsdControlMessage(pending->message, state_->wire);
            if (encoded.error != DestinationError::kOk) {
                abandon_unpublished();
                return {encoded.error, false, {}, {}};
            }
        } catch (...) {
            abandon_unpublished();
            return {DestinationError::kTransportError, false, {}, {}};
        }
    }
    std::lock_guard serial(pending->mutex);
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - std::chrono::steady_clock::now());
    if (remaining.count() <= 0) {
        auto local = SsdReadResultV1{1,
                                     pending->message.reference.attempt,
                                     true,
                                     {},
                                     state_->self.identity};
        for (size_t i = 0; i < pending->message.request->objects.size(); ++i)
            local.objects.push_back({i, AttemptOutcome::kRejected, 0, {}});
        auto result = state_->Finish(
            pending,
            {DestinationError::kOk,
             SsdControlReply{
                 SsdControlCommand::kReadInto,
                 DestinationError::kOk,
                 SsdOwnerRequestSnapshot{DestinationError::kOk,
                                         SsdOwnerRequestState::kDrained, local},
                 {}}},
            SsdControlCommand::kReadInto);
        result.may_have_submitted = false;
        return result;
    }
    pending->message.request->relative_timeout_ms = uint64_t(remaining.count());
    try {
        return state_->Finish(
            pending,
            state_->Call(*pending, SsdControlCommand::kReadInto, remaining),
            SsdControlCommand::kReadInto);
    } catch (...) {
        pending->timed_out = true;
        pending->attempt.Timeout();
        return {DestinationError::kTransportError,
                true,
                pending->message.reference.attempt,
                {}};
    }
}
SsdFixedL2ReadResult SsdFixedL2Requester::Recover(
    const AttemptIdentity& attempt, std::chrono::milliseconds timeout) {
    if (timeout.count() <= 0 || timeout > std::chrono::seconds(60)) return {};
    std::shared_ptr<State::Pending> pending;
    {
        std::lock_guard lock(state_->mutex);
        auto found = state_->pending_reads.find(Key(attempt));
        if (found == state_->pending_reads.end())
            return {DestinationError::kNotFound, false, attempt, {}};
        pending = found->second;
    }
    std::lock_guard serial(pending->mutex);
    try {
        // A concurrent recovery may have obtained this record before another
        // caller completed it and removed it from the pending map. Its trusted
        // terminal result is immutable; do not issue another control RPC or
        // Complete the destination lease twice.
        if (pending->terminal) {
            auto result = *pending->terminal;
            result.allow_fallback = false;
            return result;
        }
        return state_->Finish(
            pending,
            state_->Call(*pending, SsdControlCommand::kRecoverReadInto,
                         timeout),
            SsdControlCommand::kRecoverReadInto);
    } catch (...) {
        pending->timed_out = true;
        pending->attempt.Timeout();
        return {DestinationError::kTransportError, true, attempt, {}};
    }
}
SsdFixedL2ReadResult SsdFixedL2Requester::ReadAndDrain(
    const SsdPushPeerInfo& owner, std::vector<SsdObjectReadV1> objects,
    std::chrono::milliseconds timeout, SsdFixedL2DrainOptions options) {
    if (options.recovery_timeout.count() <= 0 ||
        options.recovery_timeout > std::chrono::seconds(60) ||
        options.retry_interval.count() <= 0 ||
        options.retry_interval > std::chrono::seconds(60))
        return {};
    auto result = Read(owner, std::move(objects), timeout);
    if (!result.may_have_submitted || result.result) return result;
    for (;;) {
        // ReadInto has already ended. Its failed attempt remains pinned, and
        // recovery uses independent short-lived RPC connections. Never turn a
        // recovery timeout, peer restart, or Close into a synthetic drain ACK.
        auto recovered = Recover(result.attempt, options.recovery_timeout);
        if (recovered.result) {
            result.result = std::move(recovered.result);
            result.allow_fallback = false;
            return result;  // The original timeout/error cannot become success.
        }
        {
            std::lock_guard lock(state_->mutex);
            // Another recovery caller may have completed and removed this
            // record. Submitted records leave this map only after a validated
            // terminal result completes the destination registry.
            if (!state_->pending_reads.contains(Key(result.attempt)))
                return result;
        }
        std::this_thread::sleep_for(options.retry_interval);
    }
}
DestinationError SsdFixedL2Requester::Revoke(const DestinationRegion& region) {
    auto error = state_->registry.BeginRevoke(region);
    if (error != DestinationError::kOk) return error;
    error = state_->registry.Retire(region);
    if (error == DestinationError::kOk) {
        std::lock_guard lock(state_->mutex);
        std::erase(state_->regions, region);
    }
    return error;
}
bool SsdFixedL2Requester::HasPendingWrite(uint64_t address,
                                          uint64_t bytes) const {
    return state_->registry.HasPendingWrite(address, bytes);
}
DestinationError SsdFixedL2Requester::Close() {
    std::vector<std::shared_ptr<State::Pending>> pending;
    std::vector<DestinationRegion> regions;
    {
        std::lock_guard lock(state_->mutex);
        state_->closing = true;
        for (const auto& [key, record] : state_->pending_reads) {
            (void)key;
            pending.push_back(record);
        }
        regions = state_->regions;
    }
    for (auto& record : pending) {
        std::lock_guard serial(record->mutex);
        record->timed_out = true;
        record->attempt.Timeout();
    }
    auto result = DestinationError::kOk;
    for (const auto& region : regions) {
        const auto error = Revoke(region);
        if (error != DestinationError::kOk &&
            error != DestinationError::kNotFound)
            result = error;
    }
    return result;
}
DestinationRegistryStats SsdFixedL2Requester::stats() const {
    return state_->registry.stats();
}
std::vector<AttemptIdentity> SsdFixedL2Requester::PendingAttempts() const {
    std::lock_guard lock(state_->mutex);
    std::vector<AttemptIdentity> result;
    for (const auto& [key, pending] : state_->pending_reads) {
        (void)key;
        result.push_back(pending->message.reference.attempt);
    }
    return result;
}
}  // namespace mooncake::ssd_offload

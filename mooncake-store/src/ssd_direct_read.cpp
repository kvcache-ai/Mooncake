// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_direct_read.h"
#include "tenant_id.h"
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <stdexcept>

namespace mooncake::ssd_offload {
namespace {
bool Valid(PeerIdentity p) {
    return (p.client_id_high || p.client_id_low) && p.boot_epoch;
}
bool ValidPeers(const std::vector<SsdFixedL2Peer>& peers) {
    if (peers.size() > 64) return false;
    for (size_t i = 0; i < peers.size(); ++i) {
        const auto& p = peers[i];
        if (!Valid(p.identity) || p.tenant_id.empty() ||
            p.tenant_id.size() > 256 || !TenantId(p.tenant_id).IsValid() ||
            p.credential == WriteCredential{} || p.transport_endpoint.empty() ||
            p.transport_endpoint.size() > 4096 ||
            p.transport_endpoint.find('\0') != std::string::npos ||
            std::any_of(peers.begin(), peers.begin() + i, [&](const auto& old) {
                return old.identity == p.identity;
            }))
            return false;
    }
    return true;
}
SsdReadResultV1 Terminal(const SsdReadRequestV1& request,
                         AttemptOutcome outcome) {
    SsdReadResultV1 result{1, request.attempt, true, {}, request.requester};
    for (size_t i = 0; i < request.objects.size(); ++i)
        result.objects.push_back({i, outcome, 0, {}});
    return result;
}
}  // namespace
struct SsdDirectReadOwner::Impl {
    Impl(PeerIdentity owner, std::vector<SsdFixedL2Peer> peers,
         SsdOwnerCallbacks::TargetFactory target,
         SsdOwnerCallbacks::Reader reader, SsdControlLimits limits,
         SsdReadProtocolLimits protocol)
        : owner(owner),
          peers(std::move(peers)),
          target(std::move(target)),
          reader(std::move(reader)),
          ledger(owner, limits, protocol, true),
          protocol(protocol) {}
    PeerIdentity owner;
    std::vector<SsdFixedL2Peer> peers;
    SsdOwnerCallbacks::TargetFactory target;
    SsdOwnerCallbacks::Reader reader;
    SsdOwnerRequestLedger ledger;
    SsdReadProtocolLimits protocol;
    std::mutex mutex;
    std::condition_variable close_cv;
    TrustedPeerResolver resolve_peer;
    size_t active_calls{0};
    bool destroyed{false};
    std::atomic<bool> closing{false};
};
SsdDirectReadOwner::SsdDirectReadOwner(PeerIdentity owner,
                                       std::vector<SsdFixedL2Peer> peers,
                                       SsdOwnerCallbacks::TargetFactory target,
                                       SsdOwnerCallbacks::Reader reader,
                                       SsdControlLimits limits,
                                       SsdReadProtocolLimits protocol,
                                       TrustedPeerResolver resolver) {
    if (!Valid(owner) || !ValidPeers(peers) || !target || !reader)
        throw std::invalid_argument(
            "Invalid fixed-L2 direct owner configuration");
    impl_ = std::make_unique<Impl>(owner, std::move(peers), std::move(target),
                                   std::move(reader), limits, protocol);
    impl_->resolve_peer = std::move(resolver);
}
SsdDirectReadOwner::~SsdDirectReadOwner() {
    {
        std::lock_guard lock(impl_->mutex);
        impl_->destroyed = true;
    }
    Close();
}
DestinationError SsdDirectReadOwner::SetPeers(
    std::vector<SsdFixedL2Peer> peers) {
    if (!ValidPeers(peers)) return DestinationError::kInvalidArgument;
    std::lock_guard lock(impl_->mutex);
    if (impl_->closing) return DestinationError::kClosing;
    if (impl_->ledger.stats().active_records != 0)
        return DestinationError::kBusy;
    // Completed payloads can still have a lost terminal reply and require
    // recovery. Refresh adds bindings; it does not revoke or rotate epochs.
    // Preflight into a copy so conflicts, capacity and allocation failures
    // cannot partially discard the authentication needed by old attempts.
    auto evict_dead_epoch = [&](std::vector<SsdFixedL2Peer>& table,
                                PeerIdentity incoming) -> bool {
        for (auto it = table.begin(); it != table.end(); ++it) {
            if (it->identity.client_id_high == incoming.client_id_high &&
                it->identity.client_id_low == incoming.client_id_low &&
                it->identity.boot_epoch < incoming.boot_epoch &&
                !impl_->ledger.HasNonDrainedRecord(it->identity)) {
                table.erase(it);
                return true;
            }
        }
        return false;
    };
    auto merged = impl_->peers;
    for (auto& peer : peers) {
        const auto known = std::find_if(
            merged.begin(), merged.end(),
            [&](const auto& old) { return old.identity == peer.identity; });
        if (known != merged.end()) {
            if (known->tenant_id != peer.tenant_id ||
                known->transport_endpoint != peer.transport_endpoint ||
                known->credential != peer.credential)
                return DestinationError::kUnauthorized;
            continue;
        }
        if (merged.size() >= 64 && !evict_dead_epoch(merged, peer.identity))
            return DestinationError::kLimit;
        merged.push_back(std::move(peer));
    }
    impl_->peers.swap(merged);
    return DestinationError::kOk;
}
DestinationError SsdDirectReadOwner::UpsertPeer(SsdFixedL2Peer peer) {
    if (!ValidPeers({peer})) return DestinationError::kInvalidArgument;
    std::lock_guard lock(impl_->mutex);
    if (impl_->closing) return DestinationError::kClosing;
    auto existing = std::find_if(
        impl_->peers.begin(), impl_->peers.end(),
        [&](const auto& old) { return old.identity == peer.identity; });
    if (existing != impl_->peers.end()) {
        // An identity/epoch has one immutable authorization binding. Existing
        // work and its recovery keep the exact originally authenticated peer.
        if (existing->tenant_id != peer.tenant_id ||
            existing->transport_endpoint != peer.transport_endpoint ||
            existing->credential != peer.credential)
            return DestinationError::kUnauthorized;
        return DestinationError::kOk;
    }
    if (impl_->peers.size() >= 64) {
        auto dead = std::find_if(
            impl_->peers.begin(), impl_->peers.end(), [&](const auto& old) {
                return old.identity.client_id_high ==
                           peer.identity.client_id_high &&
                       old.identity.client_id_low ==
                           peer.identity.client_id_low &&
                       old.identity.boot_epoch < peer.identity.boot_epoch &&
                       !impl_->ledger.HasNonDrainedRecord(old.identity);
            });
        if (dead == impl_->peers.end()) return DestinationError::kLimit;
        impl_->peers.erase(dead);
    }
    impl_->peers.push_back(std::move(peer));
    return DestinationError::kOk;
}
SsdControlReply SsdDirectReadOwner::Execute(
    const SsdControlMessage& message,
    std::chrono::steady_clock::time_point received) {
    SsdControlReply reply{
        message.command, DestinationError::kInvalidArgument, {}};
    if ((message.command != SsdControlCommand::kReadInto &&
         message.command != SsdControlCommand::kRecoverReadInto) ||
        !message.request)
        return reply;
    if (received < std::chrono::steady_clock::time_point{} ||
        received > std::chrono::steady_clock::now())
        return reply;

    struct CallGuard {
        Impl* impl{nullptr};
        ~CallGuard() {
            if (!impl) return;
            bool empty = false;
            {
                std::lock_guard lock(impl->mutex);
                empty = (--impl->active_calls == 0);
            }
            if (empty) impl->close_cv.notify_all();
        }
    } call_guard;

    {
        std::lock_guard lock(impl_->mutex);
        if (impl_->destroyed ||
            (impl_->closing.load(std::memory_order_acquire) &&
             message.command != SsdControlCommand::kRecoverReadInto)) {
            reply.error = DestinationError::kClosing;
            return reply;
        }
        ++impl_->active_calls;
        call_guard.impl = impl_.get();
    }

    const auto wire_ok =
        ValidateSsdControlMessageWireSize(message, impl_->protocol);
    if (wire_ok != DestinationError::kOk) {
        reply.error = wire_ok;
        return reply;
    }
    const auto& request = *message.request;
    if (request.attempt.owner != impl_->owner) {
        reply.error = DestinationError::kWrongAttempt;
        return reply;
    }
    std::optional<SsdFixedL2Peer> authorized_peer;
    bool from_directory = false;
    {
        std::lock_guard lock(impl_->mutex);
        const auto known = std::find_if(
            impl_->peers.begin(), impl_->peers.end(), [&](const auto& peer) {
                return peer.identity == request.requester;
            });
        if (known != impl_->peers.end()) authorized_peer = *known;
    }
    if (!authorized_peer && impl_->resolve_peer) {
        // This callback reads deployment-controlled configuration. The RPC's
        // identity is only a lookup selector; none of its inline destination
        // fields supply the authoritative endpoint or credential.
        try {
            auto resolved = impl_->resolve_peer(request.requester);
            if (resolved && resolved->identity == request.requester &&
                ValidPeers({*resolved})) {
                authorized_peer = std::move(resolved);
                from_directory = true;
            }
        } catch (...) {
            reply.error = DestinationError::kUnauthorized;
            return reply;
        }
    }
    {
        std::lock_guard lock(impl_->mutex);
        if (impl_->closing.load(std::memory_order_acquire) &&
            message.command != SsdControlCommand::kRecoverReadInto) {
            reply.error = DestinationError::kClosing;
            return reply;
        }
        const auto known = std::find_if(
            impl_->peers.begin(), impl_->peers.end(), [&](const auto& peer) {
                return peer.identity == request.requester;
            });
        if (known != impl_->peers.end())
            authorized_peer = *known;
        else if (!from_directory)
            authorized_peer.reset();
    }
    // Validate credentials, tenant, and destination interval disjointness
    // outside impl_->mutex so concurrent peers are not serialized.
    if (!authorized_peer || authorized_peer->tenant_id != request.tenant_id ||
        ((authorized_peer->credential.high ^
          message.reference.credential.high) |
         (authorized_peer->credential.low ^
          message.reference.credential.low))) {
        reply.error = DestinationError::kUnauthorized;
        return reply;
    }
    std::vector<std::pair<uint64_t, uint64_t>> intervals;
    intervals.reserve(message.destinations.size());
    for (const auto& view : message.destinations) {
        if (view.transport_endpoint != authorized_peer->transport_endpoint ||
            view.memory != DestinationMemory::kHost) {
            reply.error = DestinationError::kUnauthorized;
            return reply;
        }
        if (view.length)
            intervals.emplace_back(view.address, view.address + view.length);
    }
    std::sort(intervals.begin(), intervals.end());
    for (size_t i = 1; i < intervals.size(); ++i)
        if (intervals[i].first < intervals[i - 1].second) {
            reply.error = DestinationError::kOverlap;
            return reply;
        }
    // Cache only after authenticating the complete request binding.
    if (from_directory) {
        std::lock_guard lock(impl_->mutex);
        const auto known = std::find_if(
            impl_->peers.begin(), impl_->peers.end(), [&](const auto& peer) {
                return peer.identity == request.requester;
            });
        if (known == impl_->peers.end()) {
            if (impl_->peers.size() >= 64) {
                auto dead = std::find_if(
                    impl_->peers.begin(), impl_->peers.end(),
                    [&](const auto& old) {
                        return old.identity.client_id_high ==
                                   authorized_peer->identity.client_id_high &&
                               old.identity.client_id_low ==
                                   authorized_peer->identity.client_id_low &&
                               old.identity.boot_epoch <
                                   authorized_peer->identity.boot_epoch &&
                               !impl_->ledger.HasNonDrainedRecord(old.identity);
                    });
                if (dead != impl_->peers.end()) {
                    impl_->peers.erase(dead);
                }
            }
            if (impl_->peers.size() < 64) {
                impl_->peers.push_back(*authorized_peer);
            }
        }
    }

    if (message.command == SsdControlCommand::kRecoverReadInto) {
        auto recovered = impl_->ledger.Recover(request, message.reference,
                                               message.destinations);
        reply.error = recovered.error;
        if (reply.error == DestinationError::kOk)
            reply.snapshot = std::move(recovered);
        return reply;
    }
    auto admission = impl_->ledger.Admit(request, message.reference, received,
                                         message.destinations);
    const auto max = std::chrono::steady_clock::time_point::max();
    const auto capacity =
        std::chrono::duration_cast<std::chrono::milliseconds>(max - received)
            .count();
    const auto deadline =
        request.relative_timeout_ms >= uint64_t(capacity)
            ? max
            : received + std::chrono::milliseconds(request.relative_timeout_ms);
    if (admission.error == DestinationError::kStaleRequest) {
        auto recovered = impl_->ledger.Recover(request, message.reference,
                                               message.destinations);
        reply.error = recovered.error;
        if (reply.error == DestinationError::kOk)
            reply.snapshot = std::move(recovered);
        return reply;
    }
    if (admission.error == DestinationError::kDuplicate) {
        // Wait on the ledger condition variable (no polling loop) and return
        // the observed terminal snapshot directly to avoid eviction TOCTOU.
        auto snapshot =
            impl_->ledger.WaitForTerminal(message.reference, deadline);
        if (snapshot.error == DestinationError::kNotFound) {
            auto recovered = impl_->ledger.Recover(request, message.reference,
                                                   message.destinations);
            reply.error = recovered.error;
            if (reply.error == DestinationError::kOk)
                reply.snapshot = std::move(recovered);
            return reply;
        }
        reply.error = snapshot.error;
        if (reply.error == DestinationError::kOk && snapshot.result)
            reply.snapshot = std::move(snapshot);
        else if (reply.error == DestinationError::kOk)
            reply.error = DestinationError::kNotDrained;
        return reply;
    }
    if (admission.error != DestinationError::kOk) {
        reply.error = admission.error;
        return reply;
    }
    auto finish = [&](AttemptOutcome outcome) {
        auto result = Terminal(request, outcome);
        reply.error = admission.operation.Complete(result);
        if (reply.error == DestinationError::kOk)
            reply.snapshot = SsdOwnerRequestSnapshot{
                DestinationError::kOk, SsdOwnerRequestState::kDrained,
                std::move(result)};
        return reply;
    };
    const auto started = admission.operation.Start();
    if (started == DestinationError::kTimedOut ||
        started == DestinationError::kClosing)
        return finish(AttemptOutcome::kCancelled);
    if (started != DestinationError::kOk) {
        reply.error = started;
        return reply;
    }
    try {
        const auto anchor = std::chrono::steady_clock::now();
        const auto remaining = admission.operation.RemainingTimeout();
        if (remaining.count() <= 0) return finish(AttemptOutcome::kCancelled);
        const auto io_deadline = anchor + remaining;
        auto target = impl_->target(message.destinations);
        if (!target) return finish(AttemptOutcome::kRejected);
        if (std::chrono::steady_clock::now() >= io_deadline)
            return finish(AttemptOutcome::kCancelled);
        auto result = impl_->reader(request, target, io_deadline);
        if (!result)
            return finish(AttemptOutcome::kRejected);  // Complete preflight
                                                       // rejection, no writes.
        if (!result->dma_drained || !target->DestinationsDrained()) {
            reply.error = DestinationError::kNotDrained;
            return reply;
        }
        if (std::chrono::steady_clock::now() >= io_deadline)
            for (auto& object : result->objects)
                object.outcome = AttemptOutcome::kCancelled;
        reply.error = admission.operation.Complete(*result);
        // Complete validates this worker's result under the ledger lock. Cache
        // eviction may happen immediately afterwards; the original RPC owns
        // its terminal snapshot independently of recovery metadata retention.
        if (reply.error == DestinationError::kOk)
            reply.snapshot = SsdOwnerRequestSnapshot{
                DestinationError::kOk, SsdOwnerRequestState::kDrained,
                std::move(*result)};
        return reply;
    } catch (...) {
        // Operation destruction quarantines. Neither a thrown reader nor an
        // allocator exception is a physical destination drain acknowledgment.
        reply.error = DestinationError::kTransportError;
        return reply;
    }
}
void SsdDirectReadOwner::Close() {
    impl_->closing.store(true, std::memory_order_release);
    impl_->ledger.Close();
    std::unique_lock lock(impl_->mutex);
    impl_->close_cv.wait(lock, [&] { return impl_->active_calls == 0; });
}
SsdControlStats SsdDirectReadOwner::stats() const {
    return impl_->ledger.stats();
}
}  // namespace mooncake::ssd_offload

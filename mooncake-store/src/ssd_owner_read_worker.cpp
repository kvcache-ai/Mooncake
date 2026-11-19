// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_owner_read_worker.h"
#include "tenant_id.h"
#include <algorithm>
#include <limits>
#include <stdexcept>

namespace mooncake::ssd_offload {
SsdOwnerReadWorker::SsdOwnerReadWorker(PeerIdentity owner,
                                       std::vector<SsdOwnerRequesterPeer> peers,
                                       TargetFactory target_factory,
                                       Reader reader,
                                       SsdReadProtocolLimits limits)
    : owner_(owner),
      peers_(std::move(peers)),
      target_factory_(std::move(target_factory)),
      reader_(std::move(reader)),
      limits_(limits) {
    if ((!owner.boot_epoch ||
         (!owner.client_id_high && !owner.client_id_low)) ||
        peers_.size() > 64 || !target_factory_ || !reader_)
        throw std::invalid_argument("Invalid SSD owner worker configuration");
    for (size_t i = 0; i < peers_.size(); ++i) {
        const auto& peer = peers_[i];
        if (!peer.requester.boot_epoch ||
            (!peer.requester.client_id_high && !peer.requester.client_id_low) ||
            peer.tenant_id.empty() || peer.tenant_id.size() > 256 ||
            !TenantId(peer.tenant_id).IsValid() ||
            peer.transport_endpoint.empty() ||
            peer.transport_endpoint.size() > 4096 || !peer.resolve ||
            std::any_of(peers_.begin(), peers_.begin() + i, [&](const auto& p) {
                return p.requester == peer.requester;
            }))
            throw std::invalid_argument("Invalid SSD requester bootstrap");
    }
}
DestinationError SsdOwnerReadWorker::Run(
    const SsdReadRequestV1& request, const SsdGrantReferenceV1& reference,
    SsdOwnerRequestLedger::Operation operation) const {
    auto terminal = [&](AttemptOutcome outcome) {
        SsdReadResultV1 result{1, request.attempt, true, {}, request.requester};
        for (size_t i = 0; i < request.objects.size(); ++i)
            result.objects.push_back({i, outcome, 0, {}});
        return operation.Complete(result);
    };
    const auto started = operation.Start();
    // kClosing is an expired, still-admitted operation: this unique worker
    // has accepted no I/O. Other Start failures never synthesize a drain ACK.
    if (started == DestinationError::kClosing)
        return terminal(AttemptOutcome::kCancelled);
    if (started != DestinationError::kOk) return started;
    if (ValidateSsdReadRequestV1(request, owner_, limits_) !=
            DestinationError::kOk ||
        reference.requester != request.requester ||
        reference.attempt != request.attempt)
        return terminal(AttemptOutcome::kRejected);
    const auto peer =
        std::find_if(peers_.begin(), peers_.end(), [&](const auto& p) {
            return p.requester == request.requester &&
                   p.tenant_id == request.tenant_id;
        });
    if (peer == peers_.end()) return terminal(AttemptOutcome::kRejected);
    // Anchor before reading the remaining budget; mutex/clock overhead must
    // shorten this deadline, never extend it past the ledger's original one.
    const auto anchor = std::chrono::steady_clock::now();
    const auto remaining = operation.RemainingTimeout();
    if (remaining <= std::chrono::milliseconds::zero())
        return terminal(AttemptOutcome::kCancelled);
    const auto deadline = anchor + remaining;
    auto resolved = peer->resolve(reference, request, remaining);
    if (std::chrono::steady_clock::now() >= deadline)
        return terminal(AttemptOutcome::kCancelled);
    if (resolved.error != DestinationError::kOk)
        return terminal(AttemptOutcome::kRejected);
    size_t index = 0;
    std::vector<std::pair<uint64_t, uint64_t>> intervals;
    for (const auto& object : request.objects)
        for (const auto& range : object.ranges)
            for (const auto& destination : range.destinations) {
                if (index == resolved.destinations.size())
                    return terminal(AttemptOutcome::kRejected);
                const auto& view = resolved.destinations[index++];
                if (view.transport_endpoint != peer->transport_endpoint ||
                    view.memory != DestinationMemory::kHost ||
                    view.length != destination.length ||
                    (view.length && !view.address) ||
                    view.length >
                        std::numeric_limits<uint64_t>::max() - view.address)
                    return terminal(AttemptOutcome::kRejected);
                if (view.length)
                    intervals.emplace_back(view.address,
                                           view.address + view.length);
            }
    if (index != resolved.destinations.size())
        return terminal(AttemptOutcome::kRejected);
    std::sort(intervals.begin(), intervals.end());
    for (size_t i = 1; i < intervals.size(); ++i)
        if (intervals[i].first < intervals[i - 1].second)
            return terminal(AttemptOutcome::kRejected);
    auto target = target_factory_(std::move(resolved.destinations));
    if (std::chrono::steady_clock::now() >= deadline)
        return terminal(AttemptOutcome::kCancelled);
    if (!target) return terminal(AttemptOutcome::kRejected);
    auto result = reader_(request, target, deadline);
    if (!result) return terminal(AttemptOutcome::kRejected);
    if (!target->DestinationsDrained() || !result->dma_drained)
        return DestinationError::kNotDrained;
    // Defensive final deadline fence. A late successful read may release a
    // drained MR, but cannot make this logical Get publishable after timeout.
    if (std::chrono::steady_clock::now() >= deadline)
        for (auto& object : result->objects)
            object.outcome = AttemptOutcome::kCancelled;
    return operation.Complete(*result);
}
}  // namespace mooncake::ssd_offload

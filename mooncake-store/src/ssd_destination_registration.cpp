// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_destination_registration.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include "random.h"

namespace mooncake::ssd_offload {
struct DestinationRegistration::State {
    struct Entry {
        DestinationBinding binding;
        DestinationRegion region;
        std::shared_ptr<void> backing;
        bool logical_retired{false};
        bool in_transport{false};
        bool ownership_confirmed{false};
    };
    State(PeerIdentity requester, std::string endpoint,
          std::shared_ptr<DestinationRegistrationTransport> transport,
          DestinationRegistryLimits limits)
        : registry(requester, std::move(endpoint), limits),
          transport(std::move(transport)),
          entries(limits.max_regions) {}
    DestinationError Find(const DestinationRegion& region,
                          std::shared_ptr<Entry>& found) const {
        bool seen_id = false;
        for (const auto& entry : entries) {
            if (!entry || entry->region.region_id != region.region_id) continue;
            seen_id = true;
            if (entry->region.requester != region.requester)
                return DestinationError::kUnauthorized;
            if (entry->region.generation != region.generation) continue;
            if (entry->region.credential != region.credential)
                return DestinationError::kUnauthorized;
            found = entry;
            return DestinationError::kOk;
        }
        return seen_id ? DestinationError::kStaleRegion
                       : DestinationError::kNotFound;
    }
    mutable std::mutex mutex;
    DestinationRegistry registry;
    std::shared_ptr<DestinationRegistrationTransport> transport;
    std::vector<std::shared_ptr<Entry>> entries;
    bool closing{false};
    std::shared_ptr<State> quarantine;
};
DestinationRegistration::DestinationRegistration(
    PeerIdentity requester, std::string endpoint,
    std::shared_ptr<DestinationRegistrationTransport> transport,
    DestinationRegistryLimits limits)
    : state_(std::make_shared<State>(requester, std::move(endpoint),
                                     std::move(transport), limits)) {}
DestinationRegistration::~DestinationRegistration() {
    try {
        if (Close() == DestinationError::kOk) return;
    } catch (...) {
        // Even allocation failure during shutdown must not unmap an MR.
    }
    state_->quarantine = state_;
}
DestinationRegistry::RegistrationResult DestinationRegistration::Register(
    DestinationBinding binding, std::string location,
    std::shared_ptr<void> backing) {
    if (!backing || location.empty() || !binding.registered_base ||
        !binding.registered_bytes ||
        binding.registered_bytes >
            std::numeric_limits<uint64_t>::max() - binding.registered_base)
        return {DestinationError::kInvalidArgument, {}};
    auto entry = std::make_shared<State::Entry>();
    entry->binding = binding;
    entry->backing = std::move(backing);
    std::unique_lock lock(state_->mutex);
    if (state_->closing) return {DestinationError::kClosing, {}};
    if (!state_->transport) return {DestinationError::kNotSupported, {}};
    auto free = state_->entries.end();
    for (auto it = state_->entries.begin(); it != state_->entries.end(); ++it) {
        if (!*it) {
            if (free == state_->entries.end()) free = it;
            continue;
        }
        const auto& existing = (*it)->binding;
        if (binding.registered_base <
                existing.registered_base + existing.registered_bytes &&
            existing.registered_base <
                binding.registered_base + binding.registered_bytes)
            return {DestinationError::kOverlap, {}};
    }
    if (free == state_->entries.end()) return {DestinationError::kLimit, {}};
    WriteCredential credential;
    if (!secureRandomBytes(std::as_writable_bytes(std::span(&credential, 1))) ||
        credential == WriteCredential{})
        return {DestinationError::kNotSupported, {}};
    auto result = state_->registry.Register(binding, credential, entry);
    if (result.error != DestinationError::kOk) return result;
    entry->region = result.region;
    // All adapter/registry storage is allocated before crossing the transport
    // boundary, including the slot used for ambiguous registration failures.
    *free = entry;
    entry->in_transport = true;
    lock.unlock();
    auto registered = state_->transport->RegisterWritable(binding, location);
    lock.lock();
    entry->in_transport = false;
    entry->ownership_confirmed = registered.ownership_confirmed;
    if (registered.error == DestinationError::kOk &&
        !registered.ownership_confirmed) {
        registered.error = DestinationError::kTransportError;
        registered.cleanup_required = true;
    }
    if (registered.error == DestinationError::kOk) {
        if (!state_->closing) return result;
        state_->registry.BeginRevoke(entry->region);
        return {DestinationError::kClosing, {}};
    }
    state_->registry.BeginRevoke(entry->region);
    state_->registry.Retire(entry->region);
    entry->logical_retired = true;
    // Re-find the slot after transport callbacks, which may have changed other
    // entries. The entry itself cannot retire while in_transport is true.
    if (!registered.cleanup_required)
        for (auto& slot : state_->entries)
            if (slot == entry) {
                slot.reset();
                break;
            }
    return {registered.error, {}};
}
DestinationRegistry::AcquireResult DestinationRegistration::Acquire(
    const AttemptIdentity& identity,
    const std::vector<RegisteredDestination>& destinations) {
    std::lock_guard lock(state_->mutex);
    if (state_->closing) return {DestinationError::kClosing, {}};
    for (const auto& destination : destinations) {
        std::shared_ptr<State::Entry> entry;
        auto error = state_->Find(destination.region, entry);
        if (error != DestinationError::kOk) return {error, {}};
        if (entry->in_transport || entry->logical_retired)
            return {DestinationError::kClosing, {}};
    }
    return state_->registry.Acquire(identity, destinations);
}
DestinationError DestinationRegistration::Complete(
    const DestinationDrainAck& ack) {
    return state_->registry.Complete(ack);
}
DestinationAttemptSnapshot DestinationRegistration::Inspect(
    const AttemptIdentity& identity) const {
    return state_->registry.Inspect(identity);
}
DestinationError DestinationRegistration::ForgetRequest(PeerIdentity owner,
                                                        uint64_t request_id) {
    return state_->registry.ForgetRequest(owner, request_id);
}
DestinationError DestinationRegistration::Revoke(
    const DestinationRegion& region) {
    // Destroy backing after unlocking so allocator/mapping callbacks may
    // reenter.
    std::shared_ptr<State::Entry> entry;
    std::unique_lock lock(state_->mutex);
    auto error = state_->Find(region, entry);
    if (error != DestinationError::kOk) return error;
    if (entry->in_transport) return DestinationError::kBusy;
    if (!entry->ownership_confirmed) return DestinationError::kNotDrained;
    if (!entry->logical_retired) {
        error = state_->registry.BeginRevoke(region);
        if (error != DestinationError::kOk) return error;
        error = state_->registry.Retire(region);
        if (error != DestinationError::kOk) return error;
        entry->logical_retired = true;
    }
    entry->in_transport = true;
    lock.unlock();
    error = state_->transport->Unregister(entry->binding.registered_base);
    lock.lock();
    entry->in_transport = false;
    if (error != DestinationError::kOk) return error;
    for (auto& slot : state_->entries)
        if (slot == entry) {
            slot.reset();
            break;
        }
    return DestinationError::kOk;
}
DestinationError DestinationRegistration::Close() {
    std::vector<DestinationRegion> regions;
    {
        std::lock_guard lock(state_->mutex);
        state_->closing = true;
        regions.reserve(state_->entries.size());
        for (const auto& entry : state_->entries)
            if (entry) regions.push_back(entry->region);
    }
    auto result = DestinationError::kOk;
    for (const auto& region : regions) {
        const auto error = Revoke(region);
        // A concurrent cleanup may have already removed this entry.
        if (error != DestinationError::kOk &&
            error != DestinationError::kNotFound &&
            result == DestinationError::kOk)
            result = error;
    }
    return result;
}
size_t DestinationRegistration::pinned_regions() const {
    std::lock_guard lock(state_->mutex);
    return std::count_if(state_->entries.begin(), state_->entries.end(),
                         [](const auto& entry) { return bool(entry); });
}
}  // namespace mooncake::ssd_offload

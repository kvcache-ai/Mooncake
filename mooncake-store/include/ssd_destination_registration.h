// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <memory>
#include <string>

#include "ssd_destination_registry.h"

namespace mooncake::ssd_offload {

struct TransportRegistrationResult {
    DestinationError error{DestinationError::kNotSupported};
    // A failed registration may have partially published an MR. Keep backing
    // even though no destination handle was issued.
    bool cleanup_required{false};
    // Only an owned registration may be removed by address. Unknown ownership
    // (e.g. a failure before TE's overlap check) requires permanent quarantine.
    bool ownership_confirmed{false};
};

class DestinationRegistrationTransport {
   public:
    virtual ~DestinationRegistrationTransport() = default;
    // Called without the adapter lock. Implementations may reenter the adapter;
    // the entry remains pinned and in-progress entries return kBusy on revoke.
    virtual TransportRegistrationResult RegisterWritable(
        const DestinationBinding&, const std::string& location) noexcept = 0;
    // Success means the entire transport registration has been removed.
    virtual DestinationError Unregister(uint64_t registered_base) noexcept = 0;
};

// Explicit remote-write registration, separate from register_buffer(). Each
// entry owns a distinct MR; adopting existing registrations is not supported.
// The lifetime token must own the allocation or its parent mapping, not merely
// a wrapper around a raw application pointer. Call Close before releasing this
// object; unresolved DMA or unregister failures are conservatively quarantined.
class DestinationRegistration {
   public:
    DestinationRegistration(
        PeerIdentity requester, std::string transport_endpoint,
        std::shared_ptr<DestinationRegistrationTransport> transport,
        DestinationRegistryLimits limits = {});
    ~DestinationRegistration();
    DestinationRegistration(const DestinationRegistration&) = delete;
    DestinationRegistration& operator=(const DestinationRegistration&) = delete;

    DestinationRegistry::RegistrationResult Register(
        DestinationBinding, std::string location,
        std::shared_ptr<void> allocation_lifetime);
    DestinationRegistry::AcquireResult Acquire(
        const AttemptIdentity&, const std::vector<RegisteredDestination>&);
    DestinationError Complete(const DestinationDrainAck&);
    DestinationAttemptSnapshot Inspect(const AttemptIdentity&) const;
    DestinationError ForgetRequest(PeerIdentity, uint64_t request_id);
    // Denies new attempts immediately. Busy and transport failures are
    // retryable with this exact region; failure never permits allocation reuse.
    // A failed Register issues no region. Close retries owned cleanup; unknown
    // registration ownership stays quarantined.
    DestinationError Revoke(const DestinationRegion&);
    // Denies new work and retries owned cleanup; unknown ownership stays
    // pinned.
    DestinationError Close();
    size_t pinned_regions() const;

   private:
    struct State;
    std::shared_ptr<State> state_;
};

}  // namespace mooncake::ssd_offload

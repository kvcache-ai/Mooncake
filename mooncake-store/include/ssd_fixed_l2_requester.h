// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_requester_types.h"
#include "ssd_direct_read.h"
namespace mooncake::ssd_offload {
struct SsdFixedL2ReadResult {
    DestinationError error{DestinationError::kInvalidArgument};
    bool may_have_submitted{false};
    AttemptIdentity attempt;
    std::optional<SsdReadResultV1> result;
    // Legacy fallback is safe ONLY when `!may_have_submitted ||
    // allow_fallback`:
    //   - `!may_have_submitted`: failed locally before issuing any RPC.
    //   - `allow_fallback`: the original, timely, complete preflight rejection
    //     from the owner proved zero writes occurred.
    // A recovered/late result or uncertain timeout never permits fallback.
    bool allow_fallback{false};
};
struct SsdFixedL2DrainOptions {
    std::chrono::milliseconds recovery_timeout{1000};
    std::chrono::milliseconds retry_interval{20};
    std::chrono::milliseconds recovery_budget{1000};
};
class SsdFixedL2Requester {
   public:
    SsdFixedL2Requester(SsdFixedL2Peer self, std::string protocol,
                        SsdRequesterControlCall, DestinationRegistryLimits = {},
                        SsdReadProtocolLimits = {});
    ~SsdFixedL2Requester();
    // Trusted import of an already writable, permanently mapped CPU MR. The
    // caller keeps external L2 mappings alive for the client/engine lifetime;
    // the token pins owned pools or their registration backend through drain.
    DestinationRegistry::RegistrationResult Register(DestinationBinding,
                                                     std::shared_ptr<void>);
    SsdFixedL2ReadResult Read(const SsdPushPeerInfo& owner,
                              std::vector<SsdObjectReadV1>,
                              std::chrono::milliseconds timeout);
    // Bounded recovery for managed registrations. A pending result retains
    // the registration's actual allocation owner and is not writable data.
    // Applications must not use this adapter with a borrowed raw pointer.
    SsdFixedL2ReadResult ReadWithRecovery(const SsdPushPeerInfo& owner,
                                          std::vector<SsdObjectReadV1>,
                                          std::chrono::milliseconds timeout,
                                          SsdFixedL2DrainOptions = {});
    // Only errors/lost terminal replies enter this path. It never resubmits.
    SsdFixedL2ReadResult Recover(const AttemptIdentity&,
                                 std::chrono::milliseconds);
    DestinationError Revoke(const DestinationRegion&);
    bool HasPendingWrite(uint64_t address, uint64_t bytes) const;
    DestinationError Close();
    DestinationRegistryStats stats() const;
    std::vector<AttemptIdentity> PendingAttempts() const;

   private:
    struct State;
    std::shared_ptr<State> state_;
    SsdFixedL2ReadResult ReadInternal(const SsdPushPeerInfo& owner,
                                      std::vector<SsdObjectReadV1>,
                                      std::chrono::milliseconds timeout,
                                      std::shared_ptr<void>* out_pending);
};
}  // namespace mooncake::ssd_offload

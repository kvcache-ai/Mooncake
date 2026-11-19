// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_push_requester.h"
#include "ssd_direct_read.h"
namespace mooncake::ssd_offload {
struct SsdFixedL2ReadResult {
    DestinationError error{DestinationError::kInvalidArgument};
    bool may_have_submitted{false};
    AttemptIdentity attempt;
    std::optional<SsdReadResultV1> result;
    // Only the original, timely, complete preflight rejection permits legacy
    // fallback. A recovered/late result never starts another read attempt.
    bool allow_fallback{false};
};
struct SsdFixedL2DrainOptions {
    std::chrono::milliseconds recovery_timeout{1000};
    std::chrono::milliseconds retry_interval{20};
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
    // Safe adapter for caller-owned buffers. A logical timeout ends ReadInto,
    // then short recovery calls drain the attempt before this method returns.
    // No deadline or peer restart proves physical drain; unrecoverable writes
    // keep this worker waiting until a validated drain result is available.
    SsdFixedL2ReadResult ReadAndDrain(const SsdPushPeerInfo& owner,
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
};
}  // namespace mooncake::ssd_offload

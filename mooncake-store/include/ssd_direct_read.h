// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_control_wire.h"
#include "ssd_owner_read_worker.h"

namespace mooncake::ssd_offload {
struct SsdFixedL2Peer {
    PeerIdentity identity;
    std::string tenant_id;
    std::string transport_endpoint;
    // Installed through trusted setup, not learned from a read request.
    WriteCredential credential;
};
class SsdDirectReadOwner {
   public:
    using TrustedPeerResolver =
        std::function<std::optional<SsdFixedL2Peer>(PeerIdentity)>;
    SsdDirectReadOwner(PeerIdentity owner, std::vector<SsdFixedL2Peer>,
                       SsdOwnerReadWorker::TargetFactory,
                       SsdOwnerReadWorker::Reader, SsdControlLimits = {},
                       SsdReadProtocolLimits = {}, TrustedPeerResolver = {});
    ~SsdDirectReadOwner();
    // Run on a bounded background worker. Normal reads return only after the
    // source and destination are drained; no Resolve/Query/Forget on success.
    SsdControlReply Execute(const SsdControlMessage&,
                            std::chrono::steady_clock::time_point received_at =
                                std::chrono::steady_clock::now());
    // Atomically add trusted bindings while retaining all known epochs for
    // lost terminal reply recovery. This is not a revoke/credential-rotation
    // API; changing a known identity's tenant, endpoint or secret is rejected.
    DestinationError SetPeers(std::vector<SsdFixedL2Peer>);
    // Incremental trusted import is safe during active work, but never changes
    // the endpoint, tenant or credential of an already known identity/epoch.
    DestinationError UpsertPeer(SsdFixedL2Peer);
    void Close();
    SsdControlStats stats() const;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace mooncake::ssd_offload

// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "ssd_read_control.h"
#include "ssd_pipeline_io.h"
#include <functional>

namespace mooncake::ssd_offload {
// Installed by trusted bootstrap, never decoded from a read request. The
// resolver captures the numeric control route and epoch from that setup.
struct SsdOwnerRequesterPeer {
    PeerIdentity requester;
    std::string tenant_id;
    std::string transport_endpoint;
    std::function<SsdGrantResolution(const SsdGrantReferenceV1&,
                                     const SsdReadRequestV1&,
                                     std::chrono::milliseconds)>
        resolve;
};
class SsdOwnerReadWorker {
   public:
    using TargetFactory =
        std::function<std::shared_ptr<SourceDrainedWriteTarget>(
            std::vector<ResolvedDestination>)>;
    // nullopt means a complete preflight rejection with NO accepted writes.
    // A reader must drain accepted staging accesses even on exceptions; it
    // retains its backend/arena through return. FileStorage satisfies this
    // contract. Factory creation also accepts no writes.
    using Reader = std::function<std::optional<SsdReadResultV1>(
        const SsdReadRequestV1&, std::shared_ptr<SourceDrainedWriteTarget>,
        std::chrono::steady_clock::time_point)>;
    SsdOwnerReadWorker(PeerIdentity owner, std::vector<SsdOwnerRequesterPeer>,
                       TargetFactory, Reader, SsdReadProtocolLimits = {});
    // Run on a bounded control worker, never on an RPC I/O loop. Exceptions
    // propagate to that boundary, where operation destruction quarantines.
    DestinationError Run(const SsdReadRequestV1&, const SsdGrantReferenceV1&,
                         SsdOwnerRequestLedger::Operation) const;

   private:
    PeerIdentity owner_;
    std::vector<SsdOwnerRequesterPeer> peers_;
    TargetFactory target_factory_;
    Reader reader_;
    SsdReadProtocolLimits limits_;
};
}  // namespace mooncake::ssd_offload

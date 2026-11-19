// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_direct_read.h"
#include "ssd_push_requester.h"

namespace mooncake::ssd_offload {
struct SsdPeerDirectoryEntry {
    SsdFixedL2Peer fixed;
    SsdPushPeerInfo route;
    std::string protocol;
    bool readable{false};
};
// Deployment-owned private shared directory. Documents contain bearer secrets
// and must never be copied to public TransferEngine metadata or diagnostics.
class SsdPeerDirectory {
   public:
    struct LookupResult {
        DestinationError error{DestinationError::kNotFound};
        std::optional<SsdPeerDirectoryEntry> peer;
    };
    // The directory must exist, belong to this effective UID and grant no
    // group/other access. Symlinks and insecure peer files fail closed.
    explicit SsdPeerDirectory(std::string path);
    ~SsdPeerDirectory();
    DestinationError Publish(const SsdPeerDirectoryEntry&);
    LookupResult LookupLegacy(const std::string& endpoint,
                              const std::string& tenant,
                              const std::string& protocol) const;
    LookupResult LookupRequester(PeerIdentity, const std::string& tenant,
                                 const std::string& protocol) const;
    // Only this instance's own publication is removed. A different boot epoch
    // never grants permission to remove or replace another runtime's record.
    DestinationError Unpublish();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace mooncake::ssd_offload

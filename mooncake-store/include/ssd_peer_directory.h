// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <memory>
#include <optional>
#include <string>

#include "ssd_direct_read.h"
#include "ssd_requester_types.h"

namespace mooncake::ssd_offload {

struct SsdPeerDirectoryEntry {
    SsdFixedL2Peer fixed;
    SsdPushPeerInfo route;
    std::string protocol;
    bool readable{false};
};

// Deployment-owned private shared directory. Documents contain shared HMAC
// bootstrap secrets and must never be copied to public TransferEngine metadata
// or diagnostics. Each live publisher holds an exclusive flock on a sidecar
// lock file so crash-leftover records from SIGKILL/OOM never permanently poison
// LookupLegacy with kDuplicate.
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

    DestinationError Publish(const SsdPeerDirectoryEntry& entry);

    LookupResult LookupLegacy(const std::string& endpoint,
                              const std::string& tenant,
                              const std::string& protocol) const;
    LookupResult LookupRequester(PeerIdentity identity,
                                 const std::string& tenant,
                                 const std::string& protocol) const;

    // Only this instance's own publication is removed. A different boot epoch
    // never grants permission to remove or replace another runtime's record.
    DestinationError Unpublish();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake::ssd_offload

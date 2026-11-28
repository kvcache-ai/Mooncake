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

// Returns true iff host is a valid numeric IPv4 or IPv6 address that is not a
// wildcard bind address (0.0.0.0 / ::).
bool IsNonWildcardNumericHost(const std::string& host);

// Canonical version-1 JSON codec shared by SsdPeerDirectory and explicit
// peer-bootstrap APIs. Documents contain 128-bit shared HMAC credentials and
// must never be logged or exposed in public metrics.
std::string EncodeSsdPeerDocument(const SsdPeerDirectoryEntry& entry);
std::optional<SsdPeerDirectoryEntry> DecodeSsdPeerDocument(
    const std::string& bytes);

// Deployment-owned private local POSIX directory (single-node / co-located
// deployment where flock() is honored by the local kernel; multi-node clusters
// should use configure_ssd_push_peers over a private control plane). Documents
// contain shared HMAC bootstrap secrets and must never be copied to public
// TransferEngine metadata or diagnostics. Each live publisher holds an
// exclusive flock on a sidecar lock file so crash-leftover records from
// SIGKILL/OOM never permanently poison LookupLegacy with kDuplicate.
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

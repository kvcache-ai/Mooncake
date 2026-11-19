// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "ssd_control_wire.h"
#include <functional>
#include <span>

namespace mooncake::ssd_offload {
// Trusted local bootstrap, never supplied by a read RPC or learned from a
// transport descriptor. Legacy placement and routable control identity differ.
struct SsdPushPeerInfo {
    PeerIdentity owner;
    std::string tenant_id;
    std::string legacy_rpc_endpoint;
    std::string control_host;
    uint16_t control_port{0};
    std::string transport_endpoint;
    SsdReadCapabilityV1 capability;
};
struct SsdOwnedReadSpec {
    std::string key;  // Tenant-local key; FileStorage scopes it exactly once.
    uint64_t expected_object_size{0};
    uint64_t source_offset{0};
    uint64_t length{0};
};
struct SsdRequesterCallResult {
    DestinationError error{DestinationError::kTransportError};
    std::optional<SsdControlReply> reply;
};
using SsdRequesterControlCall = std::function<SsdRequesterCallResult(
    const std::string&, uint16_t, const SsdControlMessage&,
    std::chrono::milliseconds)>;
struct SsdPushRequesterLimits {
    uint64_t max_owned_bytes{1024ULL * 1024 * 1024};
    size_t max_owned_reads{64};
    DestinationRegistryLimits registrations{};
    SsdControlLimits grants{};
    SsdReadProtocolLimits protocol{};
};
struct SsdPushRequesterStats {
    uint64_t owned_bytes{0};
    size_t owned_reads{0};
    size_t pinned_regions{0};
};
class SsdPushRequester;
class SsdOwnedRead {
   public:
    ~SsdOwnedRead();
    SsdOwnedRead(const SsdOwnedRead&) = delete;
    SsdOwnedRead& operator=(const SsdOwnedRead&) = delete;
    // Poll never resubmits. It also accepts late drain results after Timeout.
    DestinationError Poll(std::chrono::milliseconds recovery_timeout);
    DestinationError Timeout();
    DestinationAttemptSnapshot snapshot() const;
    // Borrowed view: valid while this read is retained and before Release or
    // requester Close. A pending/failed/timed-out read never exposes bytes.
    std::optional<std::span<const std::byte>> data(size_t object_index) const;
    // A validated terminal result, or a locally drained unexposed grant with
    // Resolve fenced, permits Forget/revoke. Transient failures are retryable.
    DestinationError Release(std::chrono::milliseconds recovery_timeout);
    bool released() const;
    SsdGrantReferenceV1 reference() const;

   private:
    friend class SsdPushRequester;
    struct Impl;
    explicit SsdOwnedRead(std::unique_ptr<Impl>);
    std::unique_ptr<Impl> impl_;
};
struct SsdPushStartResult {
    DestinationError error{DestinationError::kInvalidArgument};
    // True from immediately before the Submit call, including transport
    // exceptions. False permits fallback without an accepted Push write.
    bool may_have_submitted{false};
    std::shared_ptr<SsdOwnedRead> read;
};
class SsdPushRequester {
   public:
    // One coordinator per requester UUID/boot for its complete lifetime. A
    // replacement must bootstrap a new epoch; IDs restart at one here and
    // must never reuse an existing owner replay floor for that identity.
    SsdPushRequester(PeerIdentity requester, std::string tenant_id,
                     std::string transport_endpoint,
                     std::shared_ptr<DestinationRegistrationTransport>,
                     std::vector<SsdPushPeerInfo>, SsdRequesterControlCall,
                     SsdPushRequesterLimits = {});
    ~SsdPushRequester();
    SsdPushRequester(const SsdPushRequester&) = delete;
    SsdPushRequester& operator=(const SsdPushRequester&) = delete;
    SsdPushStartResult Start(const std::string& legacy_rpc_endpoint,
                             const std::vector<SsdOwnedReadSpec>&,
                             std::chrono::milliseconds timeout);
    // Session retains uncertain attempts even if an application drops its
    // handle. One logical request per owner avoids replay-floor overtaking.
    std::shared_ptr<SsdOwnedRead> active_read(
        const std::string& legacy_rpc_endpoint) const;
    std::shared_ptr<SsdRequesterGrants> grants() const;
    SsdPushRequesterStats stats() const;
    DestinationError Close();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace mooncake::ssd_offload

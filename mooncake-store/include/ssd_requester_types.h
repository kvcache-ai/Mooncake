// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_control_wire.h"
#include <functional>
namespace mooncake::ssd_offload {
struct SsdPushPeerInfo {
    PeerIdentity owner;
    std::string tenant_id;
    std::string legacy_rpc_endpoint;
    std::string control_host;
    uint16_t control_port{0};
    std::string transport_endpoint;
    SsdReadCapabilityV1 capability;
};
struct SsdRequesterCallResult {
    DestinationError error{DestinationError::kTransportError};
    std::optional<SsdControlReply> reply;
};
using SsdRequesterControlCall = std::function<SsdRequesterCallResult(
    const std::string&, uint16_t, const SsdControlMessage&,
    std::chrono::milliseconds)>;
}  // namespace mooncake::ssd_offload

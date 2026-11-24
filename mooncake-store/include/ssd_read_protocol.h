// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <optional>
#include "ssd_destination_registry.h"

namespace mooncake::ssd_offload {
inline constexpr uint32_t kSsdReadProtocolVersion = 1;
enum SsdReadFeature : uint64_t {
    kSsdPush = 1,
    kSsdRangeRead = 2,
    kSsdScatter = 4,
    kSsdDrainAck = 8,
};
struct SsdReadProtocolLimits {
    size_t max_objects{64};
    size_t max_ranges{65536};
    size_t max_fragments{65536};
    size_t max_key_bytes{65536};
    uint64_t max_payload_bytes{1024ULL * 1024 * 1024};
    uint64_t max_timeout_ms{60000};
};
struct SsdReadCapabilityV1 {
    uint32_t protocol_version{kSsdReadProtocolVersion};
    PeerIdentity owner;
    uint64_t features{0};
    uint64_t max_batch_objects{0};
    uint64_t max_batch_bytes{0};
    uint64_t max_fragments{0};
    uint64_t max_inflight_bytes{0};
    uint64_t preferred_chunk_bytes{0};
    std::vector<std::string> supported_transports;
};
struct SsdReadRangeV1 {
    uint64_t source_offset{0};
    std::vector<RegisteredDestination> destinations;
    bool operator==(const SsdReadRangeV1&) const = default;
};
struct SsdObjectReadV1 {
    std::string key;
    uint64_t expected_object_size{0};
    std::optional<uint64_t> expected_object_checksum;
    std::vector<SsdReadRangeV1> ranges;
    bool operator==(const SsdObjectReadV1&) const = default;
};
struct SsdReadRequestV1 {
    uint32_t protocol_version{kSsdReadProtocolVersion};
    PeerIdentity requester;
    AttemptIdentity attempt;
    std::string tenant_id;
    uint64_t relative_timeout_ms{0};
    uint32_t priority{0};
    std::vector<SsdObjectReadV1> objects;
    bool operator==(const SsdReadRequestV1&) const = default;
};
struct SsdObjectReadResultV1 {
    uint64_t original_index{0};
    AttemptOutcome outcome{AttemptOutcome::kFailed};
    uint64_t valid_bytes{0};
    std::optional<uint64_t> object_checksum;
    bool operator==(const SsdObjectReadResultV1&) const = default;
};
struct SsdReadResultV1 {
    uint32_t protocol_version{kSsdReadProtocolVersion};
    AttemptIdentity attempt;
    bool dma_drained{false};
    std::vector<SsdObjectReadResultV1> objects;
    PeerIdentity requester;
    bool operator==(const SsdReadResultV1&) const = default;
};

// A partial capability never authorizes Push. Probe transport errors must be
// handled separately by the requester; they are not evidence of an old owner.
bool CanUseSsdPushV1(const SsdReadCapabilityV1&, PeerIdentity expected_owner,
                     const std::string& transport);
// Validates already decoded metadata. RPC adapters must cap wire body size
// and container allocations before decoding. Physical aliases and MR
// padding still require authoritative DestinationRegistry resolution.
DestinationError ValidateSsdReadRequestV1(const SsdReadRequestV1&,
                                          PeerIdentity expected_owner,
                                          SsdReadProtocolLimits limits = {});
// Validates terminal response shape, identity and byte counts. Checksums here
// fence metadata; the requester must verify actual destination contents before
// publishing a page. An RPC error/timeout is not a drained response.
DestinationError ValidateSsdReadResultV1(const SsdReadRequestV1&,
                                         const SsdReadResultV1&,
                                         SsdReadProtocolLimits limits = {});
}  // namespace mooncake::ssd_offload

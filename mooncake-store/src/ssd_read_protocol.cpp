// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_protocol.h"
#include "tenant_id.h"

#include <algorithm>
#include <limits>
#include <map>
#include <tuple>
#include <utility>

namespace mooncake::ssd_offload {
namespace {
bool ValidPeer(PeerIdentity peer) {
    return (peer.client_id_high || peer.client_id_low) && peer.boot_epoch;
}
bool ValidLimits(SsdReadProtocolLimits limits) {
    return limits.max_objects && limits.max_objects <= 65536 &&
           limits.max_ranges && limits.max_ranges <= 1048576 &&
           limits.max_fragments && limits.max_fragments <= 1048576 &&
           limits.max_key_bytes && limits.max_payload_bytes &&
           limits.max_timeout_ms;
}
bool KnownOutcome(AttemptOutcome outcome) {
    return outcome == AttemptOutcome::kSucceeded ||
           outcome == AttemptOutcome::kFailed ||
           outcome == AttemptOutcome::kCancelled ||
           outcome == AttemptOutcome::kRejected;
}
struct Interval {
    uint64_t region_id;
    uint64_t begin, end;
};
uint64_t ObjectBytes(const SsdObjectReadV1& object) {
    uint64_t bytes = 0;
    for (const auto& range : object.ranges)
        for (const auto& destination : range.destinations)
            bytes += destination.length;
    return bytes;
}
}  // namespace

bool CanUseSsdPushV1(const SsdReadCapabilityV1& capability,
                     PeerIdentity expected_owner,
                     const std::string& transport) {
    constexpr uint64_t required =
        kSsdPush | kSsdRangeRead | kSsdScatter | kSsdDrainAck;
    if (capability.protocol_version != kSsdReadProtocolVersion ||
        !ValidPeer(expected_owner) || capability.owner != expected_owner ||
        (capability.features & required) != required ||
        !capability.max_batch_objects || capability.max_batch_objects > 65536 ||
        !capability.max_batch_bytes || !capability.max_fragments ||
        capability.max_fragments > 1048576 || !capability.max_inflight_bytes ||
        !capability.preferred_chunk_bytes ||
        capability.preferred_chunk_bytes > capability.max_inflight_bytes ||
        capability.preferred_chunk_bytes > capability.max_batch_bytes ||
        capability.supported_transports.size() > 32 || transport.empty() ||
        transport.size() > 128)
        return false;
    for (const auto& name : capability.supported_transports)
        if (name.empty() || name.size() > 128 ||
            name.find('\0') != std::string::npos)
            return false;
    return std::find(capability.supported_transports.begin(),
                     capability.supported_transports.end(),
                     transport) != capability.supported_transports.end();
}

DestinationError ValidateSsdReadRequestV1(const SsdReadRequestV1& request,
                                          PeerIdentity expected_owner,
                                          SsdReadProtocolLimits limits) {
    if (request.protocol_version != kSsdReadProtocolVersion)
        return DestinationError::kNotSupported;
    if (!ValidPeer(request.requester) || !ValidPeer(request.attempt.owner) ||
        !ValidPeer(expected_owner) || !request.attempt.request_id ||
        !request.attempt.attempt_id || !ValidLimits(limits) ||
        request.objects.empty() || request.tenant_id.empty() ||
        request.tenant_id.size() > 256 ||
        !TenantId(request.tenant_id).IsValid() ||
        !request.relative_timeout_ms || request.priority > 2)
        return DestinationError::kInvalidArgument;
    if (request.attempt.owner != expected_owner)
        return DestinationError::kWrongAttempt;
    if (request.objects.size() > limits.max_objects ||
        request.relative_timeout_ms > limits.max_timeout_ms ||
        request.tenant_id.size() > limits.max_key_bytes)
        return DestinationError::kLimit;

    size_t keys = request.tenant_id.size(), ranges = 0, fragments = 0;
    uint64_t payload = 0;
    std::map<uint64_t, DestinationRegion> references;
    std::vector<Interval> intervals;
    for (const auto& object : request.objects) {
        if (object.key.empty() ||
            object.expected_object_size >
                uint64_t(std::numeric_limits<int64_t>::max()))
            return DestinationError::kInvalidArgument;
        if (object.key.size() > limits.max_key_bytes - keys ||
            object.ranges.size() > limits.max_ranges - ranges)
            return DestinationError::kLimit;
        keys += object.key.size();
        ranges += object.ranges.size();
        for (const auto& range : object.ranges) {
            if (range.source_offset > object.expected_object_size)
                return DestinationError::kInvalidArgument;
            if (range.destinations.size() > limits.max_fragments - fragments)
                return DestinationError::kLimit;
            fragments += range.destinations.size();
            uint64_t range_bytes = 0;
            for (const auto& destination : range.destinations) {
                const auto& region = destination.region;
                if (region.requester != request.requester)
                    return DestinationError::kStaleRegion;
                if (!region.region_id || !region.generation ||
                    (!region.credential.high && !region.credential.low))
                    return DestinationError::kInvalidArgument;
                auto [ref, inserted] =
                    references.emplace(region.region_id, region);
                if (!inserted) {
                    if (ref->second.generation != region.generation)
                        return DestinationError::kStaleRegion;
                    if (ref->second.credential != region.credential)
                        return DestinationError::kUnauthorized;
                }
                if (destination.length >
                    std::numeric_limits<uint64_t>::max() - destination.offset)
                    return DestinationError::kOverflow;
                if (destination.length > object.expected_object_size -
                                             range.source_offset - range_bytes)
                    return DestinationError::kInvalidArgument;
                if (destination.length > limits.max_payload_bytes - payload)
                    return DestinationError::kLimit;
                range_bytes += destination.length;
                payload += destination.length;
                if (destination.length)
                    intervals.push_back(
                        {region.region_id, destination.offset,
                         destination.offset + destination.length});
            }
        }
    }
    std::sort(intervals.begin(), intervals.end(),
              [](const auto& a, const auto& b) {
                  return std::tie(a.region_id, a.begin, a.end) <
                         std::tie(b.region_id, b.begin, b.end);
              });
    for (size_t i = 1; i < intervals.size(); ++i)
        if (intervals[i].region_id == intervals[i - 1].region_id &&
            intervals[i].begin < intervals[i - 1].end)
            return DestinationError::kOverlap;
    return DestinationError::kOk;
}

DestinationError ValidateSsdReadResultV1(const SsdReadRequestV1& request,
                                         const SsdReadResultV1& result,
                                         SsdReadProtocolLimits limits) {
    const auto error =
        ValidateSsdReadRequestV1(request, request.attempt.owner, limits);
    if (error != DestinationError::kOk) return error;
    if (result.protocol_version != kSsdReadProtocolVersion)
        return DestinationError::kNotSupported;
    if (result.attempt != request.attempt ||
        result.requester != request.requester)
        return DestinationError::kWrongAttempt;
    if (!result.dma_drained) return DestinationError::kNotDrained;
    if (result.objects.size() != request.objects.size())
        return DestinationError::kInvalidArgument;
    std::vector<bool> seen(request.objects.size(), false);
    for (const auto& object : result.objects) {
        if (object.original_index >= request.objects.size() ||
            seen[object.original_index] || !KnownOutcome(object.outcome))
            return DestinationError::kInvalidArgument;
        seen[object.original_index] = true;
        const auto& expected = request.objects[object.original_index];
        const auto bytes = ObjectBytes(expected);
        if (object.valid_bytes > bytes ||
            (object.outcome == AttemptOutcome::kSucceeded &&
             object.valid_bytes != bytes) ||
            (object.outcome == AttemptOutcome::kRejected &&
             object.valid_bytes) ||
            (object.outcome == AttemptOutcome::kSucceeded &&
             expected.expected_object_checksum &&
             object.object_checksum != expected.expected_object_checksum))
            return DestinationError::kInvalidArgument;
    }
    return DestinationError::kOk;
}
}  // namespace mooncake::ssd_offload

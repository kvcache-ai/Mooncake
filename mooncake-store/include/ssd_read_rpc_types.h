// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "ssd_read_protocol.h"
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake::ssd_offload {
// YLT_REFL emits unused identity parameters in generated name accessors.
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif
// Explicit V1 wire fields. Runtime bindings, lifetime tokens and registry state
// are deliberately absent; existing LOCAL_DISK reflected layouts are unchanged.
YLT_REFL(PeerIdentity, client_id_high, client_id_low, boot_epoch);
YLT_REFL(WriteCredential, high, low);
YLT_REFL(DestinationRegion, requester, region_id, generation, credential);
YLT_REFL(RegisteredDestination, region, offset, length);
YLT_REFL(AttemptIdentity, owner, request_id, attempt_id);
YLT_REFL(DestinationDrainAck, attempt, outcome, valid_bytes, dma_drained,
         requester);
YLT_REFL(SsdReadCapabilityV1, protocol_version, owner, features,
         max_batch_objects, max_batch_bytes, max_fragments, max_inflight_bytes,
         preferred_chunk_bytes, supported_transports);
YLT_REFL(SsdReadRangeV1, source_offset, destinations);
YLT_REFL(SsdObjectReadV1, key, expected_object_size, expected_object_checksum,
         ranges);
YLT_REFL(SsdReadRequestV1, protocol_version, requester, attempt, tenant_id,
         relative_timeout_ms, priority, objects);
YLT_REFL(SsdObjectReadResultV1, original_index, outcome, valid_bytes,
         object_checksum);
YLT_REFL(SsdReadResultV1, protocol_version, attempt, dma_drained, objects,
         requester);
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
}  // namespace mooncake::ssd_offload

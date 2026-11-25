// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_read_control.h"
#include <string_view>

namespace mooncake::ssd_offload {
inline constexpr size_t kSsdControlMaxWireBytes = 2 * 1024 * 1024;
enum class SsdControlCommand : uint32_t { kReadInto = 5, kRecoverReadInto = 6 };
struct SsdControlMessage {
    SsdControlCommand command{SsdControlCommand::kReadInto};
    SsdGrantReferenceV1 reference;
    std::optional<SsdReadRequestV1> request;
    // Fixed-L2 direct reads carry locally resolved views inline.
    std::vector<ResolvedDestination> destinations{};
};
struct SsdControlReply {
    SsdControlCommand command{SsdControlCommand::kReadInto};
    DestinationError error{DestinationError::kInvalidArgument};
    std::optional<SsdOwnerRequestSnapshot> snapshot;
};
struct SsdControlEncoded {
    DestinationError error{DestinationError::kInvalidArgument};
    std::string bytes;
};
// Validates a control message and verifies its serialized wire footprint fits
// within kSsdControlMaxWireBytes without allocating an output buffer.
DestinationError ValidateSsdControlMessageWireSize(const SsdControlMessage&,
                                                   SsdReadProtocolLimits = {});
SsdControlEncoded EncodeSsdControlMessage(const SsdControlMessage&,
                                          SsdReadProtocolLimits = {});
SsdControlEncoded EncodeSsdControlReply(const SsdControlReply&,
                                        SsdReadProtocolLimits = {});
// Two passes: first scan counts/string/payload bounds without allocation, then
// construct the bounded decoded value. Failure leaves output unchanged.
DestinationError DecodeSsdControlMessage(std::string_view, SsdControlMessage&,
                                         SsdReadProtocolLimits = {});
DestinationError DecodeSsdControlReply(std::string_view, SsdControlReply&,
                                       SsdReadProtocolLimits = {});
}  // namespace mooncake::ssd_offload

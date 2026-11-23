// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_read_control.h"
#include <string_view>

namespace mooncake::ssd_offload {
inline constexpr size_t kSsdControlMaxWireBytes = 2 * 1024 * 1024;
enum class SsdControlCommand : uint32_t {
    kResolve = 1,
    kSubmit = 2,
    kQuery = 3,
    kForget = 4,
    kReadInto = 5,
    kRecoverReadInto = 6
};
struct SsdControlMessage {
    SsdControlCommand command{SsdControlCommand::kQuery};
    SsdGrantReferenceV1 reference;
    std::optional<SsdReadRequestV1> request;
    // Fixed-L2 direct reads carry locally resolved views inline. Legacy
    // Submit/Resolve/Query/Forget frames remain byte-for-byte unchanged.
    std::vector<ResolvedDestination> destinations{};
};
struct SsdControlReply {
    SsdControlCommand command{SsdControlCommand::kQuery};
    DestinationError error{DestinationError::kInvalidArgument};
    std::optional<SsdOwnerRequestSnapshot> snapshot;
    std::vector<ResolvedDestination> destinations;
};
struct SsdControlEncoded {
    DestinationError error{DestinationError::kInvalidArgument};
    std::string bytes;
};
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

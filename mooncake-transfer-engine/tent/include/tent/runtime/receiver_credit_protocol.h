// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_RUNTIME_RECEIVER_CREDIT_PROTOCOL_H
#define TENT_RUNTIME_RECEIVER_CREDIT_PROTOCOL_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "tent/runtime/receiver_credit_control.h"

namespace mooncake::tent {

// A sender reports cumulative usage and asks the receiver to preserve at least
// minimum_available, and preferably desired_available, after this pull.
struct ReceiverCreditResourceUsageV1 {
    CreditResource resource{CreditResource::DataBytes};
    uint64_t consumed_total{0};
    uint64_t completed_total{0};
    uint64_t minimum_available{0};
    uint64_t desired_available{0};

    bool operator==(const ReceiverCreditResourceUsageV1& other) const {
        return resource == other.resource &&
               consumed_total == other.consumed_total &&
               completed_total == other.completed_total &&
               minimum_available == other.minimum_available &&
               desired_available == other.desired_available;
    }
};

struct ReceiverCreditPullRequestV1 {
    uint16_t schema_version{1};
    uint16_t flags{0};
    uint64_t sender_peer{0};
    uint32_t qos_class{0};
    // The first pull uses an all-zero session and epoch. Once activated, all
    // three values must exactly match the receiver response.
    ReceiverSessionId expected_receiver_session_id;
    uint64_t expected_epoch{0};
    uint64_t request_sequence{0};
    uint64_t last_update_sequence{0};
    std::vector<ReceiverCreditResourceUsageV1> resources;

    bool operator==(const ReceiverCreditPullRequestV1& other) const {
        return schema_version == other.schema_version && flags == other.flags &&
               sender_peer == other.sender_peer &&
               qos_class == other.qos_class &&
               expected_receiver_session_id ==
                   other.expected_receiver_session_id &&
               expected_epoch == other.expected_epoch &&
               request_sequence == other.request_sequence &&
               last_update_sequence == other.last_update_sequence &&
               resources == other.resources;
    }
};

enum class ReceiverCreditPullStatus : uint16_t {
    Granted = 1,
    Retry = 2,
    SessionChanged = 3,
    Unsupported = 4,
    Rejected = 5,
};

struct ReceiverCreditPullResponseV1 {
    uint16_t schema_version{1};
    uint16_t flags{0};
    ReceiverCreditPullStatus status{ReceiverCreditPullStatus::Rejected};
    uint32_t retry_after_us{0};
    CreditActivationV1 activation;
    // Allocator responses carry a full, cumulative four-resource update.
    // Unsupported/Rejected may instead use an all-zero activation/update so a
    // ControlService without an allocator can still return a typed response.
    ReceiverCreditUpdateV1 update;
};

class ReceiverCreditPullRequestCodecV1 {
   public:
    static constexpr size_t kHeaderBytes = 64;
    static constexpr size_t kResourceBytes = 36;
    static constexpr size_t kMaxWireBytes =
        kHeaderBytes + kCreditResourceCount * kResourceBytes;

    static Status validate(const ReceiverCreditPullRequestV1& request);
    static Status encode(const ReceiverCreditPullRequestV1& request,
                         std::string& wire);
    static Status decode(std::string_view wire,
                         ReceiverCreditPullRequestV1& request);
};

class ReceiverCreditPullResponseCodecV1 {
   public:
    static constexpr size_t kHeaderBytes = 100;
    static constexpr size_t kGrantBytes = 12;
    // The frame stays fixed-size for a zero payload; its grant slots become
    // validated zero padding.
    static constexpr size_t kWireBytes =
        kHeaderBytes + kCreditResourceCount * kGrantBytes;

    static Status makeZeroPayload(ReceiverCreditPullStatus status,
                                  ReceiverCreditPullResponseV1& response);
    static Status validate(const ReceiverCreditPullResponseV1& response);
    static Status encode(const ReceiverCreditPullResponseV1& response,
                         std::string& wire);
    static Status decode(std::string_view wire,
                         ReceiverCreditPullResponseV1& response);
};

}  // namespace mooncake::tent

#endif  // TENT_RUNTIME_RECEIVER_CREDIT_PROTOCOL_H

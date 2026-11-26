#pragma once

#include <functional>
#include <limits>

#include <ylt/util/tl/expected.hpp>

#include "ha/kv/ha_kv_backend.h"

namespace mooncake {

inline ErrorCode ValidateRedisWriteDurabilityPolicy(
    const KvWriteDurabilityPolicy& policy) {
    if (policy.replicas_required == 0 ||
        policy.replicas_required >
            static_cast<size_t>(std::numeric_limits<int>::max()) ||
        policy.replica_ack_timeout.count() <= 0 ||
        policy.replica_ack_timeout > std::chrono::minutes(1)) {
        return ErrorCode::INVALID_PARAMS;
    }
    // Persistent capability validation/WAITAOF belongs to the next milestone.
    // Until it exists, a persistent requirement must fail before any write.
    if (policy.require_local_aof || policy.replica_aof_required != 0) {
        return ErrorCode::NOT_SUPPORTED;
    }
    return ErrorCode::OK;
}

// Both callbacks must retain the same physical connection for their entire
// lifetime. An unknown result never causes the executor to be called again.
inline KvWriteResult ExecuteRedisWriteWithAck(
    const KvWriteDurabilityPolicy& policy,
    const std::function<KvWriteResult()>& execute,
    const std::function<tl::expected<size_t, ErrorCode>(
        size_t, std::chrono::milliseconds)>& wait_for_replicas) {
    const auto validation = ValidateRedisWriteDurabilityPolicy(policy);
    if (validation != ErrorCode::OK) {
        auto result = KvWriteResult::FromError(validation);
        result.replicas_required = policy.replicas_required;
        return result;
    }
    auto result = execute();
    result.replicas_required = policy.replicas_required;
    if (result.status != KvWriteStatus::kCommitted) {
        return result;
    }
    const auto ack =
        wait_for_replicas(policy.replicas_required, policy.replica_ack_timeout);
    if (ack) {
        result.replicas_acked = *ack;
        // N is the configured total replica membership, not a quorum. An
        // excess count is a valid WAIT reply, but mismatches this policy.
        // Count equality alone does not validate the deployment's membership.
        if (*ack == policy.replicas_required) {
            return result;
        }
    }
    result.status = KvWriteStatus::kOutcomeUnknown;
    result.error = ErrorCode::ETCD_OPERATION_ERROR;
    return result;
}

}  // namespace mooncake

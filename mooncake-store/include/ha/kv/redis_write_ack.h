#pragma once

#include <type_traits>
#include <utility>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"
#include "ha/kv/ha_kv_backend.h"

namespace mooncake {

inline ErrorCode ValidateRedisWriteDurabilityPolicy(
    const KvWriteDurabilityPolicy& policy) {
    if (!ha::IsValidRedisReplicaCount(policy.replicas_required) ||
        policy.replica_ack_timeout.count() <= 0 ||
        policy.replica_ack_timeout > std::chrono::minutes(1) ||
        policy.replica_aof_ack_timeout.count() <= 0 ||
        policy.replica_aof_ack_timeout > std::chrono::minutes(1) ||
        (policy.require_local_aof
             ? policy.replica_aof_required != policy.replicas_required
             : policy.replica_aof_required != 0)) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

// All callbacks retain the same physical connection for their entire lifetime.
// An unknown result never causes the executor to be called again.
template <typename Execute, typename Wait, typename WaitAof = std::nullptr_t>
inline KvWriteResult ExecuteRedisWriteWithAck(
    const KvWriteDurabilityPolicy& policy, Execute&& execute,
    Wait&& wait_for_replicas, WaitAof&& wait_for_aof = nullptr) {
    const auto validation = ValidateRedisWriteDurabilityPolicy(policy);
    if (validation != ErrorCode::OK) {
        auto result = KvWriteResult::FromError(validation);
        result.replicas_required = policy.replicas_required;
        return result;
    }
    if constexpr (std::is_same_v<std::decay_t<WaitAof>, std::nullptr_t>) {
        if (policy.require_local_aof) {
            auto result = KvWriteResult::FromError(ErrorCode::INVALID_PARAMS);
            result.replicas_required = policy.replicas_required;
            return result;
        }
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
            if (!policy.require_local_aof) return result;
            if constexpr (!std::is_same_v<std::decay_t<WaitAof>,
                                          std::nullptr_t>) {
                const auto aof = wait_for_aof(policy.replica_aof_required,
                                              policy.replica_aof_ack_timeout);
                if (aof) {
                    result.local_aof_acked = aof->first == 1;
                    result.replica_aof_acked = aof->second;
                    if (result.local_aof_acked &&
                        aof->second == policy.replica_aof_required) {
                        return result;
                    }
                }
            }
        }
    }
    result.status = KvWriteStatus::kOutcomeUnknown;
    result.error = ErrorCode::ETCD_OPERATION_ERROR;
    return result;
}

}  // namespace mooncake

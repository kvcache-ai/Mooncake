#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "types.h"

namespace mooncake {
namespace ha {

using ClusterNamespace = std::string;
using OwnerToken = std::string;
using OpLogSequenceId = uint64_t;
using SnapshotId = std::string;

enum class HABackendType {
    UNKNOWN = 0,
    ETCD = 1,
    REDIS = 2,
    K8S = 3,
};

inline std::string HABackendTypeToString(HABackendType type) {
    switch (type) {
        case HABackendType::UNKNOWN:
            return "unknown";
        case HABackendType::ETCD:
            return "etcd";
        case HABackendType::REDIS:
            return "redis";
        case HABackendType::K8S:
            return "k8s";
    }
    return "unknown";
}

inline std::optional<HABackendType> ParseHABackendType(std::string_view type) {
    if (type == "etcd") {
        return HABackendType::ETCD;
    }
    if (type == "redis") {
        return HABackendType::REDIS;
    }
    if (type == "k8s") {
        return HABackendType::K8S;
    }
    return std::nullopt;
}

struct HABackendSpec {
    HABackendType type = HABackendType::UNKNOWN;
    std::string connstring;
    ClusterNamespace cluster_namespace;
};

struct MasterView {
    std::string leader_address;
    ViewVersionId view_version = 0;
};

struct LeadershipSession {
    MasterView view;
    // Backend-issued opaque ownership token. Only the backend that created
    // this session is responsible for minting and interpreting it.
    OwnerToken owner_token;
    std::chrono::milliseconds lease_ttl{0};
};

enum class AcquireLeadershipStatus {
    ACQUIRED,
    CONTENDED,
};

struct AcquireLeadershipResult {
    AcquireLeadershipStatus status = AcquireLeadershipStatus::CONTENDED;
    std::optional<LeadershipSession> session;
    std::optional<MasterView> observed_view;
};

struct ViewChangeResult {
    bool changed = false;
    bool timed_out = false;
    std::optional<MasterView> current_view;
};

enum class LeadershipLossReason {
    kRenewError,
    kLostLeadership,
};

inline const char* LeadershipLossReasonToString(LeadershipLossReason reason) {
    switch (reason) {
        case LeadershipLossReason::kRenewError:
            return "renew_error";
        case LeadershipLossReason::kLostLeadership:
            return "lost_leadership";
    }
    return "unknown";
}

using LeadershipLostCallback = std::function<void(LeadershipLossReason reason)>;

struct OpLogRecord {
    OpLogSequenceId seq = 0;
    ViewVersionId producer_view_version = 0;
    std::string payload;
};

struct OpLogAppendRequest {
    OpLogSequenceId expected_next_seq = 0;
    ViewVersionId producer_view_version = 0;
    std::string payload;
};

struct OpLogPollResult {
    std::vector<OpLogRecord> records;
    OpLogSequenceId next_seq = 0;
    bool timed_out = false;
};

struct SnapshotDescriptor {
    SnapshotId snapshot_id;
    OpLogSequenceId last_included_seq = 0;
    ViewVersionId producer_view_version = 0;
    std::string manifest_key;
    std::string object_prefix;
    int64_t created_at_ms = 0;
};

}  // namespace ha
}  // namespace mooncake

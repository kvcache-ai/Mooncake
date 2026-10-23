#pragma once

#include <string>
#include <string_view>
#include <vector>

#include <boost/functional/hash.hpp>

#include "common_types.h"
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

// Error codes that mean "the object/replica already exists", i.e. an
// idempotent rewrite whose failure is surfaced as success end-to-end.
inline bool IsAlreadyExistsError(ErrorCode err) {
    return err == ErrorCode::REPLICA_NUM_EXCEEDED ||
           err == ErrorCode::REPLICA_ALREADY_EXISTS ||
           err == ErrorCode::OBJECT_ALREADY_EXISTS;
}

/**
 * @enum MemoryType
 * @brief Defines the physical storage medium type for a cache tier.
 */
enum class MemoryType { DRAM, NVME, ASCEND_NPU, UNKNOWN };

static inline std::string MemoryTypeToString(MemoryType type) {
    switch (type) {
        case MemoryType::DRAM:
            return "DRAM";
        case MemoryType::NVME:
            return "NVME";
        case MemoryType::ASCEND_NPU:
            return "ASCEND_NPU";
        default:
            return "UNKNOWN";
    }
}

/**
 * @struct ReplicaLocation
 * @brief Describes a single replica's key, tier and size.
 */
struct ReplicaLocation {
    std::string key;
    UUID tier_id;
    size_t size;
};

/**
 * @brief Election backend type for leader election in HA mode.
 */
enum class ElectionBackend { ETCD, REDIS };

static constexpr int64_t DEFAULT_CLIENT_CRASHED_TTL_SEC = 30;

enum class P2PClientServiceState {
    INITIALIZING = 0,
    ONLINE = 1,
    DEGRADED = 2,
    LOCAL_ONLY = 3,
    STOPPING = 4,
    STOPPED = 5,
};

inline const char* toString(P2PClientServiceState state) {
    switch (state) {
        case P2PClientServiceState::INITIALIZING:
            return "INITIALIZING";
        case P2PClientServiceState::ONLINE:
            return "ONLINE";
        case P2PClientServiceState::DEGRADED:
            return "DEGRADED";
        case P2PClientServiceState::LOCAL_ONLY:
            return "LOCAL_ONLY";
        case P2PClientServiceState::STOPPING:
            return "STOPPING";
        case P2PClientServiceState::STOPPED:
            return "STOPPED";
        default:
            return "UNKNOWN";
    }
}

/**
 * @brief Client health state owned by the P2P master.
 */
enum class P2PClientStatus {
    UNDEFINED = 0,
    HEALTH,
    DISCONNECTION,
    CRASHED,
};

inline std::ostream& operator<<(std::ostream& os,
                                P2PClientStatus status) noexcept {
    switch (status) {
        case P2PClientStatus::HEALTH:
            return os << "HEALTH";
        case P2PClientStatus::DISCONNECTION:
            return os << "DISCONNECTION";
        case P2PClientStatus::CRASHED:
            return os << "CRASHED";
        default:
            return os << "UNDEFINED";
    }
}

enum class P2PClientSelectionStrategy {
    ORDERED = 0,
    RANDOM = 1,
    CAPACITY_PRIORITY = 2,
};

inline std::ostream& operator<<(std::ostream& output,
                                P2PClientSelectionStrategy strategy) {
    switch (strategy) {
        case P2PClientSelectionStrategy::ORDERED:
            return output << "ORDERED";
        case P2PClientSelectionStrategy::RANDOM:
            return output << "RANDOM";
        case P2PClientSelectionStrategy::CAPACITY_PRIORITY:
            return output << "CAPACITY_PRIORITY";
    }
    return output << "UNKNOWN";
}

struct P2PWriteRouteConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;

    size_t max_candidates = 2;
    P2PClientSelectionStrategy strategy =
        P2PClientSelectionStrategy::CAPACITY_PRIORITY;
    // Remote-write weight in [0, 1]. Controls local-vs-remote routing via
    // multiplicative scoring on the master side:
    //   score = free_ratio * (is_local ? (1 - remote_weight) : remote_weight)
    //   0   -> local only  (client writes locally);
    //   0.5 -> pure capacity order (local and remote weighted equally);
    //   1   -> remote only (master never returns the local client).
    double remote_weight = 0.5;

    // Local-write waterline in [0, 1]. When the client's local utilization
    // (1 - free/total over eligible tiers) is below this threshold, the client
    // writes locally without asking the master. 0 = disabled.
    double local_write_waterline = 0.5;

    // Capacity metric used when scoring a client:
    //   false = sum free/total over all tiers;
    //   true  = only account the highest-priority eligible tier's free/total
    bool top_tier_only = true;
    bool early_return = true;  // whether to return immediately once candidates
                               // meet conditions of config

    // filter the segment with tag
    std::vector<std::string> tag_filters;
    // filter the segments whose priority is lower than priority_limit
    int priority_limit = 0;

    bool IsValid() const {
        // waterline extremes:
        //   <= 0  -> local-write bypass disabled (forbid local write)
        //   >= 1  -> always bypass to local when free (forbid remote write)
        // remote_weight extremes:
        //   <= 0  -> master only returns local routes (forbid remote routing)
        //   >= 1  -> master only returns remote routes (forbid local routing)
        // Two combinations are contradictory (dead end):
        //   forbid local write  + forbid remote routing
        //   forbid remote write + forbid local routing (defensive)
        const bool no_local_write = local_write_waterline <= 0.0;
        const bool no_remote_write = local_write_waterline >= 1.0;
        const bool no_remote_route = remote_weight <= 0.0;
        const bool no_local_route = remote_weight >= 1.0;
        return !(no_local_write && no_remote_route) &&
               !(no_remote_write && no_local_route);
    }
};
YLT_REFL(P2PWriteRouteConfig, max_candidates, strategy, remote_weight,
         local_write_waterline, top_tier_only, early_return, tag_filters,
         priority_limit);

inline std::ostream& operator<<(std::ostream& os,
                                const P2PWriteRouteConfig& config) {
    os << "P2PWriteRouteConfig: { max_candidates: " << config.max_candidates
       << ", strategy: " << config.strategy
       << ", remote_weight: " << config.remote_weight
       << ", local_write_waterline: " << config.local_write_waterline
       << ", top_tier_only: " << config.top_tier_only
       << ", early_return: " << config.early_return
       << ", priority_limit: " << config.priority_limit << " }";
    return os;
}

// Who initiates the cross-node transfer for the data plane: REVERSE matches the
// historical target-initiated path and is the conventional default when unset
// optional or client-level config omits an explicit override.
enum class TransferDirectionMode : uint8_t {
    REVERSE = 0,
    FORWARD = 1,
};

// Logging only: prints REVERSE / FORWARD / UNKNOWN for invalid numeric values.
inline std::ostream& operator<<(std::ostream& os,
                                const TransferDirectionMode& mode) noexcept {
    switch (mode) {
        case TransferDirectionMode::REVERSE:
            os << "REVERSE";
            break;
        case TransferDirectionMode::FORWARD:
            os << "FORWARD";
            break;
        default:
            os << "UNKNOWN";
            break;
    }
    return os;
}

/**
 * @brief Describes a storage segment managed by a P2P client.
 */
struct P2PSegment {
    UUID id{0, 0};
    std::string name;
    size_t size{0};
    int priority{0};
    std::vector<std::string> tags;
    MemoryType memory_type{MemoryType::DRAM};
    size_t usage{0};
};
YLT_REFL(P2PSegment, id, name, size, priority, tags, memory_type, usage);

/**
 * @brief Stable identity of a P2P route inside the master.
 *
 * Segment IDs are client-local. The pair, rather than segment_id alone, is
 * therefore the only valid identity for indexing and cleanup.
 */
struct P2PRouteLocation {
    UUID client_id{0, 0};
    UUID segment_id{0, 0};

    bool operator==(const P2PRouteLocation&) const = default;
};
YLT_REFL(P2PRouteLocation, client_id, segment_id);

struct P2PRouteLocationHash {
    size_t operator()(const P2PRouteLocation& location) const noexcept {
        size_t seed = 0;
        boost::hash_combine(seed, boost::hash<UUID>{}(location.client_id));
        boost::hash_combine(seed, boost::hash<UUID>{}(location.segment_id));
        return seed;
    }
};

struct P2PRouteEntry {
    uint64_t object_size{0};
    std::vector<P2PRouteLocation> locations;
};
YLT_REFL(P2PRouteEntry, object_size, locations);

struct P2PPublishRouteOperation {
    std::string_view key;
    uint64_t object_size{0};
    UUID segment_id{0, 0};
};
YLT_REFL(P2PPublishRouteOperation, key, object_size, segment_id);

struct P2PWithdrawRouteOperation {
    std::string_view key;
    UUID segment_id{0, 0};
};
YLT_REFL(P2PWithdrawRouteOperation, key, segment_id);

struct P2PRouteDescriptor {
    UUID client_id{0, 0};
    UUID segment_id{0, 0};
    std::string ip_address;
    uint16_t rpc_port{0};
    uint64_t object_size{0};
};
YLT_REFL(P2PRouteDescriptor, client_id, segment_id, ip_address, rpc_port,
         object_size);

struct P2PReadRouteConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;

    size_t max_candidates{RETURN_ALL_CANDIDATES};
    std::vector<std::string> tag_filters;
    int priority_limit{0};
};
YLT_REFL(P2PReadRouteConfig, max_candidates, tag_filters, priority_limit);

inline bool IsValidClusterIdComponent(const std::string& cluster_id) {
    if (cluster_id.empty()) {
        return false;
    }
    if (cluster_id.size() > 128) {
        return false;
    }
    for (unsigned char c : cluster_id) {
        const bool ok = (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') ||
                        (c >= 'a' && c <= 'z') || c == '_' || c == '-' ||
                        c == '.';
        if (!ok) {
            return false;
        }
    }
    return true;
}

// Sequence ID comparison utilities for OpLog and HA components.
// These use signed difference to handle uint64_t wrap-around correctly:
//   IsSequenceNewer(0, UINT64_MAX) = true  (0 is newer after wrap)
//   IsSequenceNewer(UINT64_MAX, 0) = false (UINT64_MAX is older before wrap)
//
// Assumes gap < 2^63, which is always true for sequence IDs in practice.
inline bool IsSequenceNewer(uint64_t a, uint64_t b) {
    return static_cast<int64_t>(a - b) > 0;
}

inline bool IsSequenceOlder(uint64_t a, uint64_t b) {
    return static_cast<int64_t>(a - b) < 0;
}

inline bool IsSequenceEqual(uint64_t a, uint64_t b) { return a == b; }

inline bool IsSequenceNewerOrEqual(uint64_t a, uint64_t b) {
    return a == b || static_cast<int64_t>(a - b) > 0;
}

inline bool IsSequenceOlderOrEqual(uint64_t a, uint64_t b) {
    return a == b || static_cast<int64_t>(a - b) < 0;
}

}  // namespace mooncake

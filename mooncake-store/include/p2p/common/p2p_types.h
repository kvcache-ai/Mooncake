#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/functional/hash.hpp>

#include "types.h"

namespace mooncake {

static constexpr int64_t DEFAULT_CLIENT_CRASHED_TTL_SEC = 30;

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

struct P2PReadRouteConfigExtra {
    std::vector<std::string> tag_filters;
    int priority_limit = 0;
};
YLT_REFL(P2PReadRouteConfigExtra, tag_filters, priority_limit);

/**
 * @brief Temporary unified-facade read selection config.
 *
 * This is not a master RPC DTO. The centralized baseline API has no
 * read-route config parameter.
 *
 * TODO(C4 / external interface; see p2p-split-plan-v2.md): Rename this to
 * P2PReadRouteConfig and remove it from centralized Query/Get/Batch methods
 * once the public client facade has architecture-specific business APIs.
 */
struct ReadRouteConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;

    ReadRouteConfig() = default;
    explicit ReadRouteConfig(size_t max_c) : max_candidates(max_c) {}

    size_t max_candidates = RETURN_ALL_CANDIDATES;
    std::optional<P2PReadRouteConfigExtra> p2p_config;
};
YLT_REFL(ReadRouteConfig, max_candidates, p2p_config);

/**
 * @brief Temporary unified-facade P2P write selection config.
 *
 * This is converted to P2PWriteRouteConfig at the master RPC boundary.
 * TODO(C4 / external interface; see p2p-split-plan-v2.md): Move this type out
 * of the shared ClientService WriteConfig after the public client facade has
 * architecture-specific business APIs.
 */
struct WriteRouteRequestConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;
    size_t max_candidates = 2;
    ObjectIterateStrategy strategy = ObjectIterateStrategy::CAPACITY_PRIORITY;
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
    bool early_return = true;

    // Exclude segments carrying any configured tag.
    std::vector<std::string> tag_filters;
    // Exclude segments whose priority is lower than this value.
    int priority_limit = 0;

    bool IsValid() const {
        const bool no_local_write = local_write_waterline <= 0.0;
        const bool no_remote_write = local_write_waterline >= 1.0;
        const bool no_remote_route = remote_weight <= 0.0;
        const bool no_local_route = remote_weight >= 1.0;
        return !(no_local_write && no_remote_route) &&
               !(no_remote_write && no_local_route);
    }
};
YLT_REFL(WriteRouteRequestConfig, max_candidates, strategy, remote_weight,
         local_write_waterline, top_tier_only, early_return, tag_filters,
         priority_limit);

inline std::ostream& operator<<(std::ostream& os,
                                const WriteRouteRequestConfig& config) {
    os << "WriteRouteRequestConfig: { max_candidates: "
       << config.max_candidates << ", strategy: " << config.strategy
       << ", remote_weight: " << config.remote_weight
       << ", local_write_waterline: " << config.local_write_waterline
       << ", top_tier_only: " << (config.top_tier_only ? "true" : "false")
       << ", early_return: " << (config.early_return ? "true" : "false")
       << ", priority_limit: " << config.priority_limit << " }";
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

}  // namespace mooncake

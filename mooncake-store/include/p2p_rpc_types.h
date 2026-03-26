#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "replica.h"
#include "types.h"
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

/**
 * @brief Request config for write route
 */
struct WriteRouteRequestConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;
    size_t max_candidates = RETURN_ALL_CANDIDATES;
    ObjectIterateStrategy strategy = ObjectIterateStrategy::CAPACITY_PRIORITY;
    bool allow_local = true;   // whether to filter local client
    bool prefer_local = true;  // enhance the priority of local client
                               // works only when allow_local==true
    bool early_return = true;  // whether to return immediately once candidates
                               // meet conditions of config

    // segment level (TODO)
    // filter the segment with tag
    std::vector<std::string> tag_filters;
    // filter the segments whose priority is lower than priority_limit
    int priority_limit = 0;
};
YLT_REFL(WriteRouteRequestConfig, max_candidates, strategy, allow_local,
         prefer_local, early_return, tag_filters, priority_limit);

inline std::ostream& operator<<(std::ostream& os,
                                const WriteRouteRequestConfig& config) {
    os << "WriteRouteRequestConfig: { max_candidates: " << config.max_candidates
       << ", strategy: " << config.strategy
       << ", allow_local: " << (config.allow_local ? "true" : "false")
       << ", prefer_local: " << (config.prefer_local ? "true" : "false")
       << ", early_return: " << (config.early_return ? "true" : "false")
       << ", priority_limit: " << config.priority_limit << " }";
    return os;
}

/**
 * @brief Request structure for getting write route.
 */
struct WriteRouteRequest {
    std::string key;  // used for pre-filter with limitation of replica number
    UUID client_id;
    size_t size = 0;
    WriteRouteRequestConfig config;
};
YLT_REFL(WriteRouteRequest, key, client_id, size, config);

/**
 * @brief Candidate node for writing route
 */
struct WriteCandidate {
    P2PProxyDescriptor replica;
    size_t available_capacity = 0;
    int priority = 0;
};
YLT_REFL(WriteCandidate, replica, available_capacity, priority);

/**
 * @brief Response structure for getting write route.
 */
struct WriteRouteResponse {
    std::vector<WriteCandidate> candidates;
};
YLT_REFL(WriteRouteResponse, candidates);

/**
 * @brief Request to add a replica
 */
struct AddReplicaRequest {
    std::string key;
    size_t size;
    P2PProxyDescriptor replica;
};
YLT_REFL(AddReplicaRequest, key, size, replica);

/**
 * @brief Request to remove a replica
 */
struct RemoveReplicaRequest {
    std::string key;
    UUID client_id;
    UUID segment_id;
    uint64_t replica_generation = 0;
};
YLT_REFL(RemoveReplicaRequest, key, client_id, segment_id, replica_generation);

/**
 * @brief One replica removal entry in a batch request.
 */
struct BatchRemoveReplicaItem {
    std::string key;
    UUID segment_id;
    uint64_t replica_generation = 0;
};
YLT_REFL(BatchRemoveReplicaItem, key, segment_id, replica_generation);

/**
 * @brief Request to remove multiple replicas in one call.
 */
struct BatchRemoveReplicaRequest {
    UUID client_id;
    std::vector<BatchRemoveReplicaItem> removals;
};
YLT_REFL(BatchRemoveReplicaRequest, client_id, removals);

}  // namespace mooncake

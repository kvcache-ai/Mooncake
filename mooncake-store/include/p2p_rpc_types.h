#pragma once

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
    bool allow_local = false;   // whether to filter local client
    bool prefer_local = false;  // enhance the priority of local client
                                // works only when allow_local==true
    bool early_return = true;   // whether to return immediately once candidates
                                // meet conditions of config

    // segment level (TODO)
    // filter the segment with tag
    std::vector<std::string> tag_filters;
    // filter the segments whose priority is lower than priority_limit
    int priority_limit = 0;
};
YLT_REFL(WriteRouteRequestConfig, max_candidates, strategy, allow_local,
         prefer_local, early_return, tag_filters, priority_limit);

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
    P2PProxyDescriptor replica;
};
YLT_REFL(RemoveReplicaRequest, key, replica);

}  // namespace mooncake

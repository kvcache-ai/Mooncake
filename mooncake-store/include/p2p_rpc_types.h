#pragma once

#include <string>
#include <string_view>
#include <optional>
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
    size_t max_candidates = 2;
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
    // used for pre-filter with limitation of replica number
    std::string_view key;
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
 * @brief Request for batch write route lookup.
 */
struct BatchGetWriteRouteRequest {
    UUID client_id;
    std::vector<std::string_view> keys;
    std::vector<size_t> sizes;
    WriteRouteRequestConfig config;  // shared config for all keys
};
YLT_REFL(BatchGetWriteRouteRequest, client_id, keys, sizes, config);

/**
 * @brief Response for batch write route lookup.
 *        responses[i] and error_codes[i] correspond to keys[i] in the request.
 */
struct BatchGetWriteRouteResponse {
    std::vector<WriteRouteResponse> responses;  // valid when error_codes[i]==OK
    std::vector<ErrorCode> error_codes;
};
YLT_REFL(BatchGetWriteRouteResponse, responses, error_codes);

/**
 * @brief Request to add a replica.
 *        Master resolves ip_address/rpc_port from registered client info.
 */
struct AddReplicaRequest {
    std::string_view key;
    size_t size;
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(AddReplicaRequest, key, size, client_id, segment_id);

/**
 * @brief Request to remove a replica
 */
struct RemoveReplicaRequest {
    std::string_view key;
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(RemoveReplicaRequest, key, client_id, segment_id);

/**
 * @brief Request to remove replicas from multiple segments in one call
 */
struct BatchRemoveReplicaRequest {
    std::string_view key;
    UUID client_id;
    std::vector<UUID> segment_ids;
};
YLT_REFL(BatchRemoveReplicaRequest, key, client_id, segment_ids);

/**
 * @brief Request to batch sync replicas (mixed ADD and REMOVE ops).
 *        Master only needs client_id + segment_id to identify replicas
 */
struct BatchSyncReplicaRequest {
    UUID client_id;
    // ADD operations
    std::vector<std::string_view> add_keys;
    std::vector<size_t> add_sizes;
    std::vector<UUID> add_segment_ids;
    // REMOVE operations
    std::vector<std::string_view> remove_keys;
    std::vector<UUID> remove_segment_ids;
};
YLT_REFL(BatchSyncReplicaRequest, client_id, add_keys, add_sizes,
         add_segment_ids, remove_keys, remove_segment_ids);

/**
 * @brief Response for batch sync replicas.
 */
struct BatchSyncReplicaResponse {
    std::vector<ErrorCode> add_results;
    std::vector<ErrorCode> remove_results;
};
YLT_REFL(BatchSyncReplicaResponse, add_results, remove_results);

}  // namespace mooncake

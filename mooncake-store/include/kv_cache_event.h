#pragma once

#include <string>

#include "ylt/struct_json/json_reader.h"

namespace mooncake {

/**
 * @brief Message struct for KV admission control in LMCache
 *
 * Represents a message for key-value admission control containing information
 * about the cache instance, worker, key and location.
 */
struct KVAdmitMsg {
    KVAdmitMsg() = default;

    std::string instance_id;  // ID of the LMCache instance
    std::string worker_id;    // Worker ID
    std::string key;          // Cache key
    std::string location;     // Location identifier

    std::string type = "KVAdmitMsg";  // Message type identifier
};
YLT_REFL(KVAdmitMsg, type, instance_id, worker_id, key, location);

/**
 * @brief Message struct for KV eviction control in LMCache
 *
 * Represents a message for key-value eviction control containing information
 * about the cache instance, worker, key and location.
 */
struct KVEvictMsg {
    KVEvictMsg() = default;

    std::string instance_id;          // ID of the LMCache instance
    std::string worker_id;            // Worker ID
    std::string key;                  // Cache key
    std::string location;             // Location identifier
    std::string type = "KVEvictMsg";  // Message type identifier
};
YLT_REFL(KVEvictMsg, type, instance_id, worker_id, key, location);

}  // namespace mooncake

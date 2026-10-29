#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "conductor/common/types.h"

namespace mooncake::conductor::kvevent {

// Loads the static service list from CONDUCTOR_CONFIG_PATH (default
// ~/.mooncake/conductor_config.json).
//  - file missing/unreadable: warn, return empty list (do not exit);
//  - JSON parse failure: log error and exit(1);
//  - unknown service type: log error, skip the entry;
//  - *http_server_port is set from the file's http_server_port field;
//    an absent field leaves the port as 0.
//  - *rpc_server_port is set from the file's rpc_server_port field;
//    an absent field keeps the caller's incoming value (an explicit 0
//    disables the RPC channel).
//  - *max_blocks_per_context (when non-null) is set from the file's
//    max_blocks_per_context field; an absent field keeps the caller's
//    incoming value, 0 disables capacity eviction, and a negative or
//    non-integer value logs an error and exits(1).
std::vector<common::ServiceConfig> ParseConfig(
    int* http_server_port, int* rpc_server_port,
    size_t* max_blocks_per_context = nullptr);

}  // namespace mooncake::conductor::kvevent

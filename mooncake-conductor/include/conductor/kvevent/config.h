#pragma once

#include <string>
#include <vector>

#include "conductor/common/types.h"

namespace conductor {
namespace kvevent {

// Loads the static service list from CONDUCTOR_CONFIG_PATH (default
// ~/.mooncake/conductor_config.json).
//  - file missing/unreadable: warn, return empty list (do not exit);
//  - JSON parse failure: log error and exit(1);
//  - unknown service type: log error, skip the entry;
//  - *http_server_port is set from the file's http_server_port field;
//    an absent field leaves the port as 0.
std::vector<common::ServiceConfig> ParseConfig(int* http_server_port);

}  // namespace kvevent
}  // namespace conductor

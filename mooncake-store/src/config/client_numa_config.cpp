#include "client_numa_config.h"

#include <cerrno>
#include <cstdlib>
#include <limits>

#include <glog/logging.h>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

ClientNumaConfig ClientNumaConfig::FromEnvironment() {
    ClientNumaConfig config;
    using Variables = ClientNumaEnvironmentVariables;

    const auto raw_value = Environ::Read(Variables::MC_STORE_NUMA_SOCKET_ID);
    if (!raw_value.has_value() || raw_value->empty()) {
        return config;
    }

    char* end_ptr = nullptr;
    errno = 0;
    const long parsed = std::strtol(raw_value->c_str(), &end_ptr, 10);
    if (errno != 0 || end_ptr == raw_value->c_str() ||
        (end_ptr != nullptr && *end_ptr != '\0') || parsed < 0 ||
        parsed > std::numeric_limits<int>::max()) {
        LOG(WARNING) << "Invalid MC_STORE_NUMA_SOCKET_ID=" << *raw_value
                     << ", falling back to auto-detect";
        return config;
    }

    config.socket_id = static_cast<int>(parsed);
    return config;
}

}  // namespace mooncake

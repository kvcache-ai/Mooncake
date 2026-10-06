#include "nof_worker_pool_config.h"

#include <glog/logging.h>

#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variables.h"

namespace mooncake {

NoFWorkerPoolConfig NoFWorkerPoolConfig::FromEnvironment() {
    NoFWorkerPoolConfig config;
    const auto raw =
        Environ::Read(NoFWorkerPoolEnvironmentVariables::MC_NOF_WORKERS);
    if (!raw || raw->empty()) {
        return config;
    }

    const auto value = TryParseEnvironmentValue<int>(*raw);
    if (value && *value > 0) {
        config.worker_count = *value;
    } else {
        LOG(WARNING) << "Invalid value for MC_NOF_WORKERS: " << *raw
                     << ", using default " << config.worker_count;
    }
    return config;
}

const NoFWorkerPoolConfig& NoFWorkerPoolConfig::AtFirstUse() {
    static const auto config = FromEnvironment();
    return config;
}

}  // namespace mooncake

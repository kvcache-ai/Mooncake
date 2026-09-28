#include "offload_parallel_worker_pool_config.h"

#include <glog/logging.h>

#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variables.h"

namespace mooncake {

OffloadParallelWorkerPoolConfig
OffloadParallelWorkerPoolConfig::FromEnvironment() {
    OffloadParallelWorkerPoolConfig config;
    const auto raw =
        Environ::Read(OffloadParallelWorkerPoolEnvironmentVariables::
                          MC_OFFLOAD_PARALLEL_WORKERS);
    if (!raw || raw->empty()) {
        return config;
    }

    const auto value = TryParseEnvironmentValue<int>(*raw);
    if (value && *value > 0) {
        config.worker_count = *value;
    } else {
        LOG(WARNING) << "Invalid value for MC_OFFLOAD_PARALLEL_WORKERS: "
                     << *raw << ", using default " << config.worker_count;
    }
    return config;
}

}  // namespace mooncake

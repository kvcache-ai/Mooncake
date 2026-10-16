#include "fileread_worker_pool_config.h"

#include <glog/logging.h>

#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variables.h"

namespace mooncake {

FilereadWorkerPoolConfig FilereadWorkerPoolConfig::FromEnvironment() {
    FilereadWorkerPoolConfig config;
    const auto raw = Environ::Read(
        FilereadWorkerPoolEnvironmentVariables::MC_FILEREAD_WORKERS);
    if (!raw || raw->empty()) {
        return config;
    }

    const auto value = TryParseEnvironmentValue<int>(*raw);
    if (value && *value > 0) {
        config.worker_count = *value;
    } else {
        LOG(WARNING) << "Invalid value for MC_FILEREAD_WORKERS: " << *raw
                     << ", using default " << config.worker_count;
    }
    return config;
}

}  // namespace mooncake

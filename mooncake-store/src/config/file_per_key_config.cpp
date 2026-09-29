#include "config/file_per_key_config.h"

#include <glog/logging.h>

#include "environ.h"
#include "client_environment_variables.h"

namespace mooncake {

bool FilePerKeyConfig::Validate() const {
    if (fsdir.empty()) {
        LOG(ERROR) << "FilePerKeyConfig: fsdir is invalid";
        return false;
    }
    return true;
}

FilePerKeyConfig FilePerKeyConfig::FromEnvironment(const Environ& env) {
    FilePerKeyConfig config;
    using Variables = ClientEnvironmentVariables::Offload::FilePerKey;

    config.fsdir =
        env.GetTypedOr(Variables::MOONCAKE_OFFLOAD_FSDIR, config.fsdir);

    config.enable_eviction = env.GetTypedOr(
        Variables::MOONCAKE_OFFLOAD_ENABLE_EVICTION,
        env.GetTypedOr(Variables::ENABLE_EVICTION, config.enable_eviction));

    return config;
}

}  // namespace mooncake

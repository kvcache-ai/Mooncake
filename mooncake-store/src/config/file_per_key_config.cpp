#include "config/file_per_key_config.h"

#include <glog/logging.h>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

bool FilePerKeyConfig::Validate() const {
    if (fsdir.empty()) {
        LOG(ERROR) << "FilePerKeyConfig: fsdir is invalid";
        return false;
    }
    return true;
}

FilePerKeyConfig FilePerKeyConfig::FromEnvironment() {
    FilePerKeyConfig config;
    using Variables = FilePerKeyEnvironmentVariables;

    config.fsdir =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_FSDIR, config.fsdir);

    config.enable_eviction = Environ::ReadOr(
        Variables::MOONCAKE_OFFLOAD_ENABLE_EVICTION,
        Environ::ReadOr(Variables::ENABLE_EVICTION, config.enable_eviction));

    return config;
}

}  // namespace mooncake

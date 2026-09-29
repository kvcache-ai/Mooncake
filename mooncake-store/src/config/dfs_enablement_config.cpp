#include "dfs_enablement_config.h"

#include "environ.h"
#include "master_environment_variables.h"

namespace mooncake {

DfsEnablementConfig DfsEnablementConfig::FromEnvironment(const Environ& env) {
    DfsEnablementConfig config;
    using Variables = MasterEnvironmentVariables::DfsEnablement;

    // Read the legacy alias first to preserve the old primary-over-alias
    // precedence and diagnostics, including an eager alias read.
    const bool legacy_enabled =
        env.GetTypedOr(Variables::MOONCAKE_DFS_ENABLED, false);
    config.enabled =
        env.GetTypedOr(Variables::MOONCAKE_ENABLE_DFS, legacy_enabled);
    return config;
}

}  // namespace mooncake

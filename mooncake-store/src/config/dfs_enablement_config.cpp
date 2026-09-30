#include "dfs_enablement_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

DfsEnablementConfig DfsEnablementConfig::FromEnvironment() {
    DfsEnablementConfig config;
    using Variables = DfsEnablementEnvironmentVariables;

    // Read the legacy alias first to preserve the old primary-over-alias
    // precedence and diagnostics, including an eager alias read.
    const bool legacy_enabled =
        Environ::ReadOr(Variables::MOONCAKE_DFS_ENABLED, false);
    config.enabled =
        Environ::ReadOr(Variables::MOONCAKE_ENABLE_DFS, legacy_enabled);
    return config;
}

}  // namespace mooncake

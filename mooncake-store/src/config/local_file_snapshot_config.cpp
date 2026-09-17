#include "local_file_snapshot_config.h"

#include <stdexcept>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

LocalFileSnapshotConfig LocalFileSnapshotConfig::FromEnvironment() {
    const auto value = Environ::Read(
        LocalFileSnapshotEnvironmentVariables::MOONCAKE_SNAPSHOT_LOCAL_PATH);
    if (!value || value->empty()) {
        throw std::runtime_error(
            "MOONCAKE_SNAPSHOT_LOCAL_PATH environment variable is not set. "
            "Please set it to a persistent directory path for snapshot "
            "storage. Example: export "
            "MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/mooncake_snapshots");
    }
    return LocalFileSnapshotConfig{.base_path = *value};
}

}  // namespace mooncake

#pragma once

#include <string>

namespace mooncake {

struct LocalFileSnapshotConfig {
    std::string base_path;

    static LocalFileSnapshotConfig FromEnvironment();
};

}  // namespace mooncake

#pragma once

#include <string>

namespace mooncake {

class Environ;

struct LocalFileSnapshotConfig {
    std::string base_path;

    static LocalFileSnapshotConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake

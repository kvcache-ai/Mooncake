#pragma once

namespace mooncake {

class Environ;

struct ClientObjectChecksumConfig {
    bool enabled = false;

    static bool IsEnabledAtFirstUse();
    static ClientObjectChecksumConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake

#pragma once

namespace mooncake {

class Environ;

struct DfsEnablementConfig {
    bool enabled = false;

    static DfsEnablementConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake

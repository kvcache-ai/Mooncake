#pragma once

namespace mooncake {

class Environ;

struct ShmSpdkRegistrationConfig {
    bool enabled = false;

    static ShmSpdkRegistrationConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake

#pragma once

namespace mooncake {

struct ShmSpdkRegistrationConfig {
    bool enabled = false;

    static ShmSpdkRegistrationConfig FromEnvironment();
};

}  // namespace mooncake

#pragma once

namespace mooncake {

struct ClientObjectChecksumConfig {
    bool enabled = false;

    static bool IsEnabledAtFirstUse();
    static ClientObjectChecksumConfig FromEnvironment();
};

}  // namespace mooncake

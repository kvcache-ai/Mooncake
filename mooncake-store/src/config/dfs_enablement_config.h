#pragma once

namespace mooncake {

struct DfsEnablementConfig {
    bool enabled = false;

    static DfsEnablementConfig FromEnvironment();
};

}  // namespace mooncake

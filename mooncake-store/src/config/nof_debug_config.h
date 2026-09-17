#pragma once

namespace mooncake {

struct NoFDebugConfig {
    bool enabled = false;
    int interval_ms = 1000;

    static bool ReadEnabledFromEnvironment();
    static int ReadIntervalMsFromEnvironment();
    static bool IsEnabledAtFirstUse();
    static int IntervalMsAtFirstUse();
    static NoFDebugConfig FromEnvironment();
};

}  // namespace mooncake

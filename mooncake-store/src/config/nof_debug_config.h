#pragma once

#include <chrono>

namespace mooncake {

struct NoFDebugConfig {
    bool enabled = false;
    std::chrono::milliseconds interval_ms{1000};

    static bool IsEnabledAtFirstUse();
    static std::chrono::milliseconds IntervalMsAtFirstUse();
    static NoFDebugConfig FromEnvironment();

   private:
    static bool ReadEnabledFromEnvironment();
    static std::chrono::milliseconds ReadIntervalMsFromEnvironment();
};

}  // namespace mooncake

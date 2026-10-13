#pragma once

#include <chrono>

namespace mooncake {

class Environ;

struct NoFDebugConfig {
    bool enabled = false;
    std::chrono::milliseconds interval_ms{1000};

    static bool IsEnabledAtFirstUse();
    static std::chrono::milliseconds IntervalMsAtFirstUse();
    static NoFDebugConfig FromEnvironment(const Environ& env);

   private:
    static bool ReadEnabledFromEnvironment(const Environ& env);
    static std::chrono::milliseconds ReadIntervalMsFromEnvironment(
        const Environ& env);
};

}  // namespace mooncake

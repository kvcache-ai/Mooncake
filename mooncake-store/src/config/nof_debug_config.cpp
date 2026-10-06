#include "nof_debug_config.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

bool NoFDebugConfig::ReadEnabledFromEnvironment() {
    const auto raw = Environ::Read(NoFDebugEnvironmentVariables::MC_NOF_DEBUG);
    if (!raw) return false;

    std::string normalized = *raw;
    std::transform(
        normalized.begin(), normalized.end(), normalized.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return normalized == "1" || normalized == "true" || normalized == "yes" ||
           normalized == "on";
}

std::chrono::milliseconds NoFDebugConfig::ReadIntervalMsFromEnvironment() {
    const auto raw =
        Environ::Read(NoFDebugEnvironmentVariables::MC_NOF_DEBUG_INTERVAL_MS);
    if (!raw) return std::chrono::milliseconds{1000};

    char* end = nullptr;
    const long parsed = std::strtol(raw->c_str(), &end, 10);
    if (end == raw->c_str() || (end != nullptr && *end != '\0') ||
        parsed <= 0) {
        return std::chrono::milliseconds{1000};
    }
    return std::chrono::milliseconds{static_cast<int>(parsed)};
}

bool NoFDebugConfig::IsEnabledAtFirstUse() {
    static const bool enabled = ReadEnabledFromEnvironment();
    return enabled;
}

std::chrono::milliseconds NoFDebugConfig::IntervalMsAtFirstUse() {
    static const auto interval_ms = ReadIntervalMsFromEnvironment();
    return interval_ms;
}

NoFDebugConfig NoFDebugConfig::FromEnvironment() {
    return {ReadEnabledFromEnvironment(), ReadIntervalMsFromEnvironment()};
}

}  // namespace mooncake

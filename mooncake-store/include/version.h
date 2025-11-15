#pragma once

#include <string>

namespace mooncake {

// Version information for the Mooncake system
constexpr const char* MOONCAKE_VERSION = "1.0.0";

// Function to get the version string
inline const std::string& GetMooncakeVersion() {
    static const std::string version(MOONCAKE_VERSION);
    return version;
}

}  // namespace mooncake

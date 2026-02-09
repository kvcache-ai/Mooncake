#pragma once

#include <string>

namespace mooncake {

// Version information for the Mooncake system
constexpr const char* MOONCAKE_STORE_VERSION = "2.0.0";

// Function to get the version string
inline const std::string& GetMooncakeStoreVersion() {
    static const std::string version(MOONCAKE_STORE_VERSION);
    return version;
}

}  // namespace mooncake

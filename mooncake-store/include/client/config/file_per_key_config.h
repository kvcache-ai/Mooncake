#pragma once

#include <string>

namespace mooncake {

struct FilePerKeyConfig {
    std::string fsdir = "file_per_key_dir";  // Subdirectory name

    bool enable_eviction = true;  // Enable eviction for storage

    bool Validate() const;

    static FilePerKeyConfig FromEnvironment();
};

}  // namespace mooncake

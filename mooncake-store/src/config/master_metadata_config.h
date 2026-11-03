#pragma once

#include <string>

namespace mooncake {

struct MasterMetadataConfig {
    std::string cluster_id;

    static MasterMetadataConfig FromEnvironment();
    std::string HttpMetadataPrefix() const;
};

}  // namespace mooncake

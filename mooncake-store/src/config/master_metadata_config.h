#pragma once

#include <string>

namespace mooncake {

class Environ;

struct MasterMetadataConfig {
    std::string cluster_id;

    static MasterMetadataConfig FromEnvironment(const Environ& env);
    std::string HttpMetadataPrefix() const;
};

}  // namespace mooncake

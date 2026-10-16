#include "master_metadata_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

MasterMetadataConfig MasterMetadataConfig::FromEnvironment() {
    MasterMetadataConfig config;
    config.cluster_id =
        Environ::Read(
            MasterMetadataEnvironmentVariables::MC_METADATA_CLUSTER_ID)
            .value_or("");
    return config;
}

std::string MasterMetadataConfig::HttpMetadataPrefix() const {
    if (cluster_id.empty()) {
        return "mooncake/";
    }

    std::string prefix = "mooncake/" + cluster_id;
    if (prefix.back() != '/') {
        prefix += '/';
    }
    return prefix;
}

}  // namespace mooncake

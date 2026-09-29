#include "master_metadata_config.h"

#include "environ.h"
#include "master_environment_variables.h"

namespace mooncake {

MasterMetadataConfig MasterMetadataConfig::FromEnvironment(const Environ& env) {
    MasterMetadataConfig config;
    config.cluster_id =
        env.GetTyped(
               MasterEnvironmentVariables::Metadata::MC_METADATA_CLUSTER_ID)
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

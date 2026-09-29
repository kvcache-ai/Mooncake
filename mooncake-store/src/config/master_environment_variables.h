#pragma once

// Environment variables read only by the Mooncake Store master process.
// Variables that the client also reads are declared in
// mooncake-common/src/environment_variables.h.

#include <cstdint>
#include <string>

#include "environment_variable.h"

namespace mooncake {

struct MasterEnvironmentVariables {
    struct DfsEnablement {
        MC_DEFINE_ENV_VAR(bool, MOONCAKE_ENABLE_DFS);
        MC_DEFINE_ENV_VAR(bool, MOONCAKE_DFS_ENABLED);
    };
    struct Metadata {
        MC_DEFINE_ENV_VAR(std::string, MC_METADATA_CLUSTER_ID);
    };
    struct Snapshot {
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_SNAPSHOT_LOCAL_PATH);
    };
    struct S3Client {
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_AWS_REGION);
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_AWS_S3_ENDPOINT);
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_AWS_BUCKET_NAME);
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_AWS_ACCESS_KEY_ID);
        MC_DEFINE_ENV_VAR(std::string, MOONCAKE_AWS_SECRET_ACCESS_KEY);
        MC_DEFINE_ENV_VAR(bool, MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING);
        MC_DEFINE_ENV_VAR(bool, MOONCAKE_AWS_USE_HTTPS);
        MC_DEFINE_ENV_VAR(std::string,
                          MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION);
        MC_DEFINE_ENV_VAR(std::string,
                          MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION);
        MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_AWS_CONNECT_TIMEOUT_MS);
        MC_DEFINE_ENV_VAR(int64_t, MOONCAKE_AWS_REQUEST_TIMEOUT_MS);
    };
};

}  // namespace mooncake

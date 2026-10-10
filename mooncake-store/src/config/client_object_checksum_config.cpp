#include "client_object_checksum_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

bool ClientObjectChecksumConfig::IsEnabledAtFirstUse() {
    static const bool enabled = FromEnvironment(Environ::Process()).enabled;
    return enabled;
}

ClientObjectChecksumConfig ClientObjectChecksumConfig::FromEnvironment(
    const Environ& env) {
    return {env.GetTypedOr(
        ClientObjectChecksumEnvironmentVariables::MOONCAKE_STORE_CHECKSUM,
        false)};
}

}  // namespace mooncake

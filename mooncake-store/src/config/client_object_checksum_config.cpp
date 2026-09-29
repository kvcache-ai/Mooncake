#include "client_object_checksum_config.h"

#include "environ.h"
#include "client_environment_variables.h"

namespace mooncake {

bool ClientObjectChecksumConfig::IsEnabledAtFirstUse() {
    static const bool enabled = FromEnvironment(Environ::Process()).enabled;
    return enabled;
}

ClientObjectChecksumConfig ClientObjectChecksumConfig::FromEnvironment(
    const Environ& env) {
    return {env.GetTypedOr(
        ClientEnvironmentVariables::ObjectChecksum::MOONCAKE_STORE_CHECKSUM,
        false)};
}

}  // namespace mooncake

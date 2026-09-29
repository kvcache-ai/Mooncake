#include "shm_spdk_registration_config.h"

#include "environ.h"
#include "client_environment_variables.h"

namespace mooncake {

ShmSpdkRegistrationConfig ShmSpdkRegistrationConfig::FromEnvironment(
    const Environ& env) {
    return {env.GetTyped(ClientEnvironmentVariables::ShmSpdkRegistration::
                             MC_STORE_REGISTER_SPDK) == "1"};
}

}  // namespace mooncake

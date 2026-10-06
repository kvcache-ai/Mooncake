#include "shm_spdk_registration_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

ShmSpdkRegistrationConfig ShmSpdkRegistrationConfig::FromEnvironment() {
    return {
        Environ::Read(
            ShmSpdkRegistrationEnvironmentVariables::MC_STORE_REGISTER_SPDK) ==
        "1"};
}

}  // namespace mooncake

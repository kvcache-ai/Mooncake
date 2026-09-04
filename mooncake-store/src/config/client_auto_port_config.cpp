#include "client_auto_port_config.h"

#include "config.h"
#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

ClientAutoPortConfig ClientAutoPortConfig::FromEnvironment() {
    ClientAutoPortConfig config;
    using Variables = ClientAutoPortEnvironmentVariables;

    config.max_retries = Environ::ReadOr(
        Variables::MC_STORE_CLIENT_SETUP_RETRIES, config.max_retries);
    const int raw_min_port =
        Environ::ReadOr(Variables::MC_STORE_CLIENT_MIN_PORT, config.min_port);
    const int raw_max_port =
        Environ::ReadOr(Variables::MC_STORE_CLIENT_MAX_PORT, config.max_port);

    const auto [min_port, max_port] = ValidatePortRange(
        raw_min_port, raw_max_port, config.min_port, config.max_port);
    config.min_port = min_port;
    config.max_port = max_port;
    return config;
}

}  // namespace mooncake

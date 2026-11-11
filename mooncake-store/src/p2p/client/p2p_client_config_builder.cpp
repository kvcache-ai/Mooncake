#include "p2p/client/p2p_client_config_builder.h"

#include "environ.h"

namespace mooncake {

std::string P2PClientConfigBuilder::ResolveRuntimeConfigSource(
    const std::string& runtime_config) {
    if (!runtime_config.empty()) {
        return runtime_config;
    }
    return Environ::Process().Get("MC_RUNTIME_CONFIG").value_or("");
}

}  // namespace mooncake

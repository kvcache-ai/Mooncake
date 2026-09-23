#include "runtime_identity_config_loader.h"

#include <cstdlib>

#include "default_config.h"

namespace mooncake {

RuntimeIdentityConfig ResolveRuntimeIdentityConfig(
    const DefaultConfig* file_config,
    const RuntimeIdentityCommandLineOverrides& command_line) {
    RuntimeIdentityConfig result;

    if (file_config != nullptr) {
        if (file_config->Contains("pod_name")) {
            file_config->GetString("pod_name", &result.pod_name);
        }
        if (file_config->Contains("pod_namespace")) {
            file_config->GetString("pod_namespace", &result.pod_namespace);
        }
    }

    if (command_line.pod_name.has_value()) {
        result.pod_name = *command_line.pod_name;
    }
    if (command_line.pod_namespace.has_value()) {
        result.pod_namespace = *command_line.pod_namespace;
    }

    // Match the existing K8s Downward API fallback, including explicit empty
    // file and command-line values.
    if (result.pod_name.empty()) {
        if (const char* env = std::getenv("POD_NAME")) {
            result.pod_name = env;
        }
    }
    if (result.pod_namespace.empty()) {
        if (const char* env = std::getenv("POD_NAMESPACE")) {
            result.pod_namespace = env;
        }
    }

    return result;
}

}  // namespace mooncake

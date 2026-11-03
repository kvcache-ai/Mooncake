#include "nof_register_config.h"

#include <glog/logging.h>

#include <algorithm>
#include <cctype>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

NoFRegisterConfig NoFRegisterConfig::FromEnvironment() {
    NoFRegisterConfig config;
    config.transport_type =
        Environ::Read(NoFRegisterEnvironmentVariables::MC_NOF_TRTYPE)
            .value_or("RDMA");
    std::transform(config.transport_type.begin(), config.transport_type.end(),
                   config.transport_type.begin(), [](unsigned char c) {
                       return static_cast<char>(std::toupper(c));
                   });
    if (config.transport_type != "RDMA" && config.transport_type != "TCP") {
        LOG(WARNING) << "Invalid MC_NOF_TRTYPE=" << config.transport_type
                     << ", fallback to RDMA";
        config.transport_type = "RDMA";
    }
    return config;
}

}  // namespace mooncake

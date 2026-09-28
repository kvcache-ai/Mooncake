#include "nof_qos_config.h"

#include <glog/logging.h>

#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variables.h"

namespace mooncake {
namespace {

int PositiveBytesOrDefault(const EnvironmentVariable<std::string>& variable,
                           int default_value) {
    const auto raw = Environ::Read(variable);
    if (!raw || raw->empty()) {
        return default_value;
    }

    const auto value = TryParseEnvironmentValue<int>(*raw);
    if (value && *value > 0) {
        return *value;
    }
    LOG(WARNING) << "Invalid value for " << variable.name << ": " << *raw
                 << ", using default " << default_value;
    return default_value;
}

}  // namespace

NoFQosConfig NoFQosConfig::FromEnvironment() {
    NoFQosConfig config;
    config.submit_chunk_bytes = PositiveBytesOrDefault(
        NoFQosEnvironmentVariables::MC_NOF_SUBMIT_CHUNK_BYTES,
        config.submit_chunk_bytes);
    config.inflight_bytes_limit = PositiveBytesOrDefault(
        NoFQosEnvironmentVariables::MC_NOF_INFLIGHT_BYTES_LIMIT,
        config.inflight_bytes_limit);
    return config;
}

const NoFQosConfig& NoFQosConfig::AtFirstUse() {
    static const auto config = FromEnvironment();
    return config;
}

}  // namespace mooncake

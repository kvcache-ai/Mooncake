#include "transfer_submitter_config.h"

#include <glog/logging.h>

#include <algorithm>
#include <cctype>

#include "bool_parser.h"
#include "environ.h"
#include "environment_variables.h"

namespace mooncake {
namespace {

std::optional<bool> ReadMemcpyOverride() {
    auto value =
        Environ::Read(TransferSubmitterEnvironmentVariables::MC_STORE_MEMCPY);
    if (!value.has_value()) {
        return std::nullopt;
    }

    std::transform(value->begin(), value->end(), value->begin(),
                   [](unsigned char c) { return std::tolower(c); });
    const auto parsed = TryParseBool(*value, {.trim_ascii_whitespace = false});
    // Reject enable/disable, which only the shared parser accepts.
    if (parsed.has_value() && *value != "enable" && *value != "disable") {
        return *parsed;
    }
    LOG(WARNING) << "Invalid value for MC_STORE_MEMCPY: " << *value
                 << ", defaulting to enabled";
    return true;
}

std::optional<std::chrono::milliseconds> ReadTransferTimeoutOverride() {
    const auto& variable =
        TransferSubmitterEnvironmentVariables::MC_STORE_TRANSFER_TIMEOUT_MS;
    const auto raw = Environ::Process().Get(variable.name);
    if (!raw.has_value()) {
        return std::nullopt;
    }

    const auto parsed = Environ::Read(variable);
    if (parsed.has_value() &&
        *parsed >= TransferSubmitterConfig::kMinTransferTimeoutMs &&
        *parsed <= TransferSubmitterConfig::kMaxTransferTimeoutMs) {
        return std::chrono::milliseconds(*parsed);
    }
    LOG(WARNING) << "Invalid value for MC_STORE_TRANSFER_TIMEOUT_MS: '" << *raw
                 << "', expected an integer in ["
                 << TransferSubmitterConfig::kMinTransferTimeoutMs << ", "
                 << TransferSubmitterConfig::kMaxTransferTimeoutMs
                 << "]; keeping the default timeout";
    return std::nullopt;
}

}  // namespace

TransferSubmitterConfig TransferSubmitterConfig::FromEnvironment() {
    TransferSubmitterConfig config;
    config.memcpy_enabled_override = ReadMemcpyOverride();
    config.transfer_timeout_override = ReadTransferTimeoutOverride();
    return config;
}

}  // namespace mooncake

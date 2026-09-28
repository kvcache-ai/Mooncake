#include "local_hot_cache.h"

#include <glog/logging.h>

#include <exception>
#include <optional>
#include <string>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {
namespace {

size_t ParseLegacyPositiveSizeOr(const std::optional<std::string>& raw_value,
                                 size_t default_value,
                                 const char* variable_name,
                                 const char* fallback_message) {
    if (!raw_value.has_value()) {
        return default_value;
    }
    const std::string error_message = "Invalid " + std::string(variable_name) +
                                      "='" + *raw_value + "'" +
                                      fallback_message;
    if (!raw_value->empty() && raw_value->front() == '-') {
        LOG(WARNING) << error_message;
        return default_value;
    }

    try {
        // Keep the legacy numeric-prefix behavior of std::stoull.
        const unsigned long long value = std::stoull(*raw_value, nullptr, 10);
        if (value > 0) {
            return static_cast<size_t>(value);
        }
    } catch (const std::exception&) {
    }

    LOG(WARNING) << error_message;
    return default_value;
}

}  // namespace

LocalHotCacheConfig LocalHotCacheConfig::FromEnvironment() {
    LocalHotCacheConfig config;
    using Variables = LocalHotCacheEnvironmentVariables;

    const auto total_size =
        Environ::Read(Variables::MC_STORE_LOCAL_HOT_CACHE_SIZE);
    config.total_size_bytes = ParseLegacyPositiveSizeOr(
        total_size, 0, Variables::MC_STORE_LOCAL_HOT_CACHE_SIZE.name,
        ", disable local hot cache");
    if (config.total_size_bytes == 0) {
        return config;
    }

    const auto block_size =
        Environ::Read(Variables::MC_STORE_LOCAL_HOT_BLOCK_SIZE);
    config.block_size_bytes =
        ParseLegacyPositiveSizeOr(block_size, config.block_size_bytes,
                                  Variables::MC_STORE_LOCAL_HOT_BLOCK_SIZE.name,
                                  ", using default block size");

    config.use_shm = Environ::Read(Variables::MC_STORE_LOCAL_HOT_CACHE_USE_SHM)
                         .value_or(std::string{}) == "1";

    const auto admission_threshold =
        Environ::Read(Variables::MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD);
    if (admission_threshold.has_value()) {
        const std::string error_message =
            "Invalid " +
            std::string(
                Variables::MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD.name) +
            "='" + *admission_threshold + "', using default";
        try {
            const unsigned long long value =
                std::stoull(*admission_threshold, nullptr, 10);
            if (value > 0 && value <= 255) {
                config.admission_threshold = static_cast<uint8_t>(value);
            } else {
                LOG(WARNING) << error_message;
            }
        } catch (const std::exception&) {
            LOG(WARNING) << error_message;
        }
    }

    return config;
}

}  // namespace mooncake

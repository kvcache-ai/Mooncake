#include "conductor/common/utils.h"

#include <glog/logging.h>

#include <cstdlib>
#include <string>

#include "ascii_string.h"
#include "integer_parser.h"

namespace mooncake::conductor::common {

LogLevelConfig ParseLogLevel() {
    constexpr LogLevelConfig kInfo{google::GLOG_INFO, 0};

    const char* level_env = std::getenv("CONDUCTOR_LOG_LEVEL");
    const std::string level_str = level_env ? level_env : "";
    if (level_str.empty()) {
        return kInfo;
    }

    const std::string lower = AsciiToLower(level_str);
    if (lower == "debug") return {google::GLOG_INFO, 1};
    if (lower == "info") return kInfo;
    if (lower == "warn") return {google::GLOG_WARNING, 0};
    if (lower == "error") return {google::GLOG_ERROR, 0};

    LOG(WARNING) << "Invalid log level specified, defaulting to INFO"
                 << " level=" << level_str;
    return kInfo;
}

std::string LoadEnv(const std::string& env_name,
                    const std::string& default_env) {
    const char* value = std::getenv(env_name.c_str());
    if (value == nullptr || value[0] == '\0') {
        LOG(WARNING) << "environment variable is not set, using default value"
                     << " envName=" << env_name
                     << " defaultValue=" << default_env;
        return default_env;
    }
    return value;
}

int LoadIntEnv(const std::string& env_name, int default_env) {
    const char* raw = std::getenv(env_name.c_str());
    const std::string value = raw ? raw : "";
    if (!value.empty()) {
        // Strict on purpose: no surrounding whitespace, no trailing garbage.
        const auto parsed =
            TryParseInteger<int>(value, {.allow_leading_plus = true});
        if (parsed.has_value()) {
            return *parsed;
        }
        LOG(ERROR) << "invalid value for environment variable"
                   << " envName=" << env_name << " value=" << value;
    }
    LOG(WARNING) << "environment variable is not set, using default value"
                 << " envName=" << env_name << " defaultValue=" << default_env;
    return default_env;
}

}  // namespace mooncake::conductor::common

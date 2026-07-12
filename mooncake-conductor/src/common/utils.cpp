#include "conductor/common/utils.h"

#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string>

namespace conductor {
namespace common {

namespace {

std::string ToUpper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return s;
}

// Strict integer parse: optional sign, digits only, no leading/trailing
// whitespace (std::stoi would skip leading whitespace, which is rejected
// here).
bool AtoiStrict(const std::string& s, int* out) {
    if (s.empty()) return false;
    size_t i = (s[0] == '+' || s[0] == '-') ? 1 : 0;
    if (i == s.size()) return false;
    for (size_t j = i; j < s.size(); ++j) {
        if (!std::isdigit(static_cast<unsigned char>(s[j]))) return false;
    }
    try {
        size_t pos = 0;
        const int v = std::stoi(s, &pos);
        if (pos != s.size()) return false;
        *out = v;
        return true;
    } catch (const std::exception&) {
        return false;  // out of int range
    }
}

}  // namespace

LogLevel ParseLogLevel() {
    const char* level_env = std::getenv("CONDUCTOR_LOG_LEVEL");
    const std::string level_str = level_env ? level_env : "";
    if (level_str.empty()) {
        return LogLevel::kInfo;
    }

    const std::string upper = ToUpper(level_str);
    if (upper == "DEBUG") return LogLevel::kDebug;
    if (upper == "INFO") return LogLevel::kInfo;
    if (upper == "WARN") return LogLevel::kWarn;
    if (upper == "ERROR") return LogLevel::kError;

    LOG(WARNING) << "Invalid log level specified, defaulting to INFO"
                 << " level=" << level_str;
    return LogLevel::kInfo;
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
        int int_value = 0;
        if (AtoiStrict(value, &int_value)) {
            return int_value;
        }
        LOG(ERROR) << "invalid value for environment variable"
                   << " envName=" << env_name << " value=" << value;
    }
    LOG(WARNING) << "environment variable is not set, using default value"
                 << " envName=" << env_name << " defaultValue=" << default_env;
    return default_env;
}

}  // namespace common
}  // namespace conductor

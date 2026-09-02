#pragma once

// Utility functions for environment variable parsing and log-level
// configuration.

#include <string>

namespace conductor {
namespace common {

enum class LogLevel { kDebug, kInfo, kWarn, kError };

// Reads CONDUCTOR_LOG_LEVEL (DEBUG/INFO/WARN/ERROR, case-insensitive).
// Empty -> INFO; invalid values warn and fall back to INFO.
LogLevel ParseLogLevel();

// Returns env var `env_name`, or `default_env` (with a warning) when the
// variable is unset or empty.
std::string LoadEnv(const std::string& env_name,
                    const std::string& default_env);

// Returns env var `env_name` parsed as int. Unparsable values log an
// error and fall through to the default (with a warning).
int LoadIntEnv(const std::string& env_name, int default_env);

}  // namespace common
}  // namespace conductor

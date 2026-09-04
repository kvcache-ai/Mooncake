#pragma once

// Conductor-private helpers.

#include <string>

namespace mooncake::conductor::common {

struct LogLevelConfig {
    int min_severity;
    int verbosity;

    bool operator==(const LogLevelConfig&) const = default;
};

LogLevelConfig ParseLogLevel();

std::string LoadEnv(const std::string& env_name,
                    const std::string& default_env);

int LoadIntEnv(const std::string& env_name, int default_env);

}  // namespace mooncake::conductor::common

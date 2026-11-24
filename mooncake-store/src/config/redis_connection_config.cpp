#include "redis_connection_config.h"

#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variables.h"

namespace mooncake {

tl::expected<RedisConnectionConfig, ErrorCode>
RedisConnectionConfig::FromEnvironment() {
    RedisConnectionConfig config;
    const auto integer = [](const auto& variable, int& value, int minimum,
                            int maximum) {
        const auto raw = Environ::Read(variable);
        if (!raw || raw->empty()) return true;
        const auto parsed = TryParseEnvironmentValue<int>(*raw);
        if (!parsed || *parsed < minimum || *parsed > maximum) return false;
        value = *parsed;
        return true;
    };
    if (!integer(
            RedisConnectionEnvironmentVariables::MC_REDIS_CONNECT_TIMEOUT_MS,
            config.connect_timeout_ms, 1, 60000) ||
        !integer(
            RedisConnectionEnvironmentVariables::MC_REDIS_COMMAND_TIMEOUT_MS,
            config.command_timeout_ms, 1, 60000) ||
        !integer(
            RedisConnectionEnvironmentVariables::MC_REDIS_TOPOLOGY_REFRESH_MS,
            config.topology_refresh_ms, 0, 60000) ||
        !integer(RedisConnectionEnvironmentVariables::MC_REDIS_MAX_REDIRECTS,
                 config.max_redirects, 0, 64))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    const auto cluster =
        Environ::Read(
            RedisConnectionEnvironmentVariables::MC_REDIS_CLUSTER_ENABLED)
            .value_or("");
    if (!cluster.empty()) {
        if (cluster != "0" && cluster != "1")
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        config.cluster_enabled = cluster == "1";
    }
    const auto raw_db_index =
        Environ::Read(RedisConnectionEnvironmentVariables::MC_REDIS_DB_INDEX)
            .value_or("");
    if (!raw_db_index.empty()) {
        const auto db_index = TryParseEnvironmentValue<int>(raw_db_index);
        if (!db_index.has_value() || *db_index < 0 || *db_index > 255) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        config.db_index = *db_index;
    }
    config.username =
        Environ::Read(RedisConnectionEnvironmentVariables::MC_REDIS_USERNAME)
            .value_or("");
    config.password =
        Environ::Read(RedisConnectionEnvironmentVariables::MC_REDIS_PASSWORD)
            .value_or("");
    return config;
}

}  // namespace mooncake

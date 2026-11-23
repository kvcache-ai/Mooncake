#include "redis_connection_config.h"

#include <charconv>
#include <glog/logging.h>

#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variables.h"

namespace mooncake {

tl::expected<RedisConnectionConfig, ErrorCode>
RedisConnectionConfig::FromEnvironment() {
    RedisConnectionConfig config;
    const auto profile = Environ::Read(
        RedisConnectionEnvironmentVariables::MC_REDIS_DURABILITY_PROFILE);
    if (profile) {
        if (*profile == "persistent") {
            config.durability_profile = RedisDurabilityProfile::kPersistent;
        } else if (*profile != "memory") {
            LOG(ERROR)
                << "MC_REDIS_DURABILITY_PROFILE must be memory or persistent";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    const auto ack_timeout = [](const auto& variable, int& value) {
        const auto raw = Environ::Read(variable);
        if (!raw) return true;
        int parsed = 0;
        const auto result =
            std::from_chars(raw->data(), raw->data() + raw->size(), parsed);
        if (result.ec != std::errc{} ||
            result.ptr != raw->data() + raw->size() || parsed < 1 ||
            parsed > 60000) {
            LOG(ERROR) << variable.name
                       << " must be a decimal integer in [1, 60000]";
            return false;
        }
        value = parsed;
        return true;
    };
    if (!ack_timeout(RedisConnectionEnvironmentVariables::
                         MC_REDIS_REPLICATION_ACK_TIMEOUT_MS,
                     config.replication_ack_timeout_ms) ||
        !ack_timeout(
            RedisConnectionEnvironmentVariables::MC_REDIS_AOF_ACK_TIMEOUT_MS,
            config.aof_ack_timeout_ms)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
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

#include "redis_connection_config.h"

#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variables.h"

namespace mooncake {

tl::expected<RedisConnectionConfig, ErrorCode>
RedisConnectionConfig::FromEnvironment() {
    RedisConnectionConfig config;
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

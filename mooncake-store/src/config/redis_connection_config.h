#pragma once

#include <string>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

class Environ;

struct RedisConnectionConfig {
    int db_index = 0;
    std::string username;
    std::string password;

    static tl::expected<RedisConnectionConfig, ErrorCode> FromEnvironment(
        const Environ& env);
};

}  // namespace mooncake

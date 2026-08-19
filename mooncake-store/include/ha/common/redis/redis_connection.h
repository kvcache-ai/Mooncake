#pragma once

#include <memory>
#include <string>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

struct redisContext;
struct redisReply;

namespace mooncake {
namespace ha {
namespace common {
namespace redis {

struct RedisEndpoint {
    std::string host;
    int port = 6379;
};

struct RedisContextDeleter {
    void operator()(redisContext* context) const;
};

using RedisContextPtr = std::unique_ptr<redisContext, RedisContextDeleter>;

struct RedisReplyDeleter {
    void operator()(redisReply* reply) const;
};

using RedisReplyPtr = std::unique_ptr<redisReply, RedisReplyDeleter>;

bool IsStringReply(const redisReply* reply);

tl::expected<int, ErrorCode> ParsePositiveInt(std::string_view text,
                                              int min_value, int max_value);

tl::expected<int, ErrorCode> ResolveRedisDbIndex();

tl::expected<RedisEndpoint, ErrorCode> ParseRedisEndpoint(
    std::string_view connstring);

std::string SanitizeHashTagComponent(std::string component);

tl::expected<RedisContextPtr, ErrorCode> ConnectRedis(
    std::string_view connstring,
    ErrorCode connection_error = ErrorCode::PERSISTENT_FAIL);

}  // namespace redis
}  // namespace common
}  // namespace ha
}  // namespace mooncake

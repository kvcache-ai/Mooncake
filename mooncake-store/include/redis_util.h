#pragma once

#ifdef STORE_USE_REDIS

#include <memory>
#include <string>

#include <hiredis/hiredis.h>

namespace mooncake {

/// Custom deleter for redisReply* — used by RedisReplyPtr.
struct RedisReplyDeleter {
    void operator()(redisReply* reply) const {
        if (reply) freeReplyObject(reply);
    }
};

/// RAII wrapper for redisReply* — automatically calls freeReplyObject on
/// destruction. Usage: RedisReplyPtr reply((redisReply*)redisCommand(...));
using RedisReplyPtr = std::unique_ptr<redisReply, RedisReplyDeleter>;

namespace RedisUtil {

bool ParseEndpoint(const std::string& redis_endpoint, std::string& host,
                   int& port, bool require_explicit_port = false);

redisContext* CreateConnection(const std::string& redis_endpoint,
                               const std::string& username,
                               const std::string& password, int db_index = 0,
                               int connect_timeout_ms = 5000,
                               int command_timeout_ms = 3000,
                               bool require_explicit_port = false);

}  // namespace RedisUtil

}  // namespace mooncake

#endif  // STORE_USE_REDIS

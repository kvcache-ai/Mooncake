#pragma once

#ifdef STORE_USE_REDIS

#include <string>

#include <hiredis/hiredis.h>

namespace mooncake::testing {

inline bool AuthenticateRedisContext(redisContext* ctx,
                                     const std::string& username,
                                     const std::string& password) {
    if (!ctx || ctx->err) {
        return false;
    }

    redisReply* reply = nullptr;
    if (!username.empty()) {
        reply = static_cast<redisReply*>(
            redisCommand(ctx, "AUTH %b %b", username.data(), username.size(),
                         password.data(), password.size()));
    } else if (!password.empty()) {
        reply = static_cast<redisReply*>(
            redisCommand(ctx, "AUTH %b", password.data(), password.size()));
    } else {
        return true;
    }

    bool ok = reply && reply->type != REDIS_REPLY_ERROR;
    if (reply) {
        freeReplyObject(reply);
    }
    return ok;
}

}  // namespace mooncake::testing

#endif  // STORE_USE_REDIS

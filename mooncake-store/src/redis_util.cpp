#ifdef STORE_USE_REDIS

#include "redis_util.h"

#include <glog/logging.h>

#include <algorithm>
#include <utility>

namespace mooncake {
namespace {

bool ParseUint64(const std::string& value, uint64_t& result) {
    try {
        size_t pos = 0;
        result = std::stoull(value, &pos);
        return pos == value.size();
    } catch (...) {
        return false;
    }
}

}  // namespace

namespace RedisUtil {

bool ParseEndpoint(const std::string& redis_endpoint, std::string& host,
                   int& port) {
    if (redis_endpoint.empty()) {
        return false;
    }

    std::string parsed_host;
    uint64_t parsed_port = 0;
    if (redis_endpoint.front() == '[') {
        auto bracket_pos = redis_endpoint.find(']');
        if (bracket_pos == std::string::npos) {
            return false;
        }
        parsed_host = redis_endpoint.substr(1, bracket_pos - 1);
        if (bracket_pos + 1 >= redis_endpoint.size() ||
            redis_endpoint[bracket_pos + 1] != ':') {
            return false;
        }
        const std::string port_str = redis_endpoint.substr(bracket_pos + 2);
        if (port_str.empty() || !ParseUint64(port_str, parsed_port) ||
            parsed_port == 0 || parsed_port > 65535) {
            return false;
        }
    } else {
        const auto colon_count =
            std::count(redis_endpoint.begin(), redis_endpoint.end(), ':');
        if (colon_count != 1) {
            return false;
        }
        auto colon_pos = redis_endpoint.find(':');
        parsed_host = redis_endpoint.substr(0, colon_pos);
        const std::string port_str = redis_endpoint.substr(colon_pos + 1);
        if (parsed_host.empty() || port_str.empty() ||
            !ParseUint64(port_str, parsed_port) || parsed_port == 0 ||
            parsed_port > 65535) {
            return false;
        }
    }
    if (parsed_host.empty()) {
        return false;
    }
    host = std::move(parsed_host);
    port = static_cast<int>(parsed_port);
    return true;
}

redisContext* CreateConnection(const std::string& redis_endpoint,
                               const std::string& username,
                               const std::string& password, int db_index,
                               int connect_timeout_ms, int command_timeout_ms) {
    std::string host;
    int port = 0;
    if (!ParseEndpoint(redis_endpoint, host, port)) {
        LOG(ERROR) << "RedisUtil: invalid Redis endpoint"
                   << ", endpoint=" << redis_endpoint;
        return nullptr;
    }

    struct timeval connect_timeout;
    connect_timeout.tv_sec = connect_timeout_ms / 1000;
    connect_timeout.tv_usec = (connect_timeout_ms % 1000) * 1000;

    redisContext* ctx =
        redisConnectWithTimeout(host.c_str(), port, connect_timeout);
    if (!ctx || ctx->err) {
        LOG(ERROR) << "RedisUtil: failed to connect to Redis"
                   << ", endpoint=" << redis_endpoint
                   << ", error=" << (ctx ? ctx->errstr : "null");
        if (ctx) {
            redisFree(ctx);
        }
        return nullptr;
    }

    struct timeval command_timeout;
    command_timeout.tv_sec = command_timeout_ms / 1000;
    command_timeout.tv_usec = (command_timeout_ms % 1000) * 1000;
    redisSetTimeout(ctx, command_timeout);

    if (!username.empty()) {
        RedisReplyPtr auth((redisReply*)redisCommand(
            ctx, "AUTH %b %b", username.data(), username.size(),
            password.data(), password.size()));
        if (!auth || auth->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "RedisUtil: AUTH failed"
                       << ", endpoint=" << redis_endpoint << ", error="
                       << (auth && auth->str ? auth->str : "null");
            redisFree(ctx);
            return nullptr;
        }
    } else if (!password.empty()) {
        RedisReplyPtr auth((redisReply*)redisCommand(
            ctx, "AUTH %b", password.data(), password.size()));
        if (!auth || auth->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "RedisUtil: AUTH failed"
                       << ", endpoint=" << redis_endpoint << ", error="
                       << (auth && auth->str ? auth->str : "null");
            redisFree(ctx);
            return nullptr;
        }
    }

    if (db_index != 0) {
        RedisReplyPtr select(
            (redisReply*)redisCommand(ctx, "SELECT %d", db_index));
        if (!select || select->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "RedisUtil: SELECT failed"
                       << ", endpoint=" << redis_endpoint
                       << ", db_index=" << db_index << ", error="
                       << (select && select->str ? select->str : "null");
            redisFree(ctx);
            return nullptr;
        }
    }

    return ctx;
}

}  // namespace RedisUtil
}  // namespace mooncake

#endif  // STORE_USE_REDIS

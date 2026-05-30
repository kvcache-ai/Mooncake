#include "ha/common/redis/redis_connection.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <exception>

#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#endif

namespace mooncake {
namespace ha {
namespace common {
namespace redis {

namespace {

constexpr auto kRedisConnectTimeout = std::chrono::seconds(3);
constexpr auto kRedisCommandTimeout = std::chrono::seconds(3);

}  // namespace

#ifdef STORE_USE_REDIS

void RedisContextDeleter::operator()(redisContext* context) const {
    if (context != nullptr) {
        redisFree(context);
    }
}

void RedisReplyDeleter::operator()(redisReply* reply) const {
    if (reply != nullptr) {
        freeReplyObject(reply);
    }
}

bool IsStringReply(const redisReply* reply) {
    return reply != nullptr && (reply->type == REDIS_REPLY_STRING ||
                                reply->type == REDIS_REPLY_STATUS);
}

#else

void RedisContextDeleter::operator()(redisContext* context) const {
    (void)context;
}

void RedisReplyDeleter::operator()(redisReply* reply) const { (void)reply; }

bool IsStringReply(const redisReply* reply) {
    (void)reply;
    return false;
}

#endif

tl::expected<int, ErrorCode> ParsePositiveInt(std::string_view text,
                                              int min_value, int max_value) {
    if (text.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    try {
        const auto parsed = std::stoll(std::string(text));
        if (parsed < min_value || parsed > max_value) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return static_cast<int>(parsed);
    } catch (const std::exception&) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
}

tl::expected<int, ErrorCode> ResolveRedisDbIndex() {
    const char* raw_db_index = std::getenv("MC_REDIS_DB_INDEX");
    if (raw_db_index == nullptr || std::strlen(raw_db_index) == 0) {
        return 0;
    }
    return ParsePositiveInt(raw_db_index, 0, 255);
}

tl::expected<RedisEndpoint, ErrorCode> ParseRedisEndpoint(
    std::string_view connstring) {
    constexpr std::string_view kRedisScheme = "redis://";
    if (connstring.substr(0, kRedisScheme.size()) == kRedisScheme) {
        connstring.remove_prefix(kRedisScheme.size());
    }

    const auto slash_pos = connstring.find('/');
    if (slash_pos != std::string_view::npos) {
        if (slash_pos + 1 != connstring.size()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        connstring = connstring.substr(0, slash_pos);
    }

    if (connstring.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    RedisEndpoint endpoint;
    if (connstring.front() == '[') {
        const auto bracket_pos = connstring.find(']');
        if (bracket_pos == std::string_view::npos || bracket_pos == 1) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        endpoint.host = std::string(connstring.substr(1, bracket_pos - 1));
        const auto remainder = connstring.substr(bracket_pos + 1);
        if (remainder.empty()) {
            return endpoint;
        }
        if (remainder.front() != ':') {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        auto port = ParsePositiveInt(remainder.substr(1), 1, 65535);
        if (!port) {
            return tl::make_unexpected(port.error());
        }
        endpoint.port = port.value();
        return endpoint;
    }

    const auto colon_pos = connstring.rfind(':');
    if (colon_pos == std::string_view::npos) {
        endpoint.host = std::string(connstring);
        return endpoint;
    }

    if (connstring.find(':') != colon_pos) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    endpoint.host = std::string(connstring.substr(0, colon_pos));
    if (endpoint.host.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto port = ParsePositiveInt(connstring.substr(colon_pos + 1), 1, 65535);
    if (!port) {
        return tl::make_unexpected(port.error());
    }
    endpoint.port = port.value();
    return endpoint;
}

std::string SanitizeHashTagComponent(std::string component) {
    if (!component.empty() && component.back() == '/') {
        component.pop_back();
    }
    std::replace(component.begin(), component.end(), '{', '_');
    std::replace(component.begin(), component.end(), '}', '_');
    return component;
}

#ifndef STORE_USE_REDIS

tl::expected<RedisContextPtr, ErrorCode> ConnectRedis(
    std::string_view connstring, ErrorCode connection_error) {
    (void)connstring;
    (void)connection_error;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

#else

tl::expected<RedisContextPtr, ErrorCode> ConnectRedis(
    std::string_view connstring, ErrorCode connection_error) {
    auto endpoint = ParseRedisEndpoint(connstring);
    if (!endpoint) {
        return tl::make_unexpected(endpoint.error());
    }

    auto db_index = ResolveRedisDbIndex();
    if (!db_index) {
        return tl::make_unexpected(db_index.error());
    }

    timeval connect_timeout{};
    connect_timeout.tv_sec = static_cast<long>(kRedisConnectTimeout.count());
    connect_timeout.tv_usec = 0;
    RedisContextPtr context(redisConnectWithTimeout(
        endpoint->host.c_str(), endpoint->port, connect_timeout));
    if (context == nullptr || context->err != 0) {
        return tl::make_unexpected(connection_error);
    }

    timeval command_timeout{};
    command_timeout.tv_sec = static_cast<long>(kRedisCommandTimeout.count());
    command_timeout.tv_usec = 0;
    if (redisSetTimeout(context.get(), command_timeout) != REDIS_OK) {
        return tl::make_unexpected(connection_error);
    }

    const char* username = std::getenv("MC_REDIS_USERNAME");
    const char* password = std::getenv("MC_REDIS_PASSWORD");
    if (password != nullptr && std::strlen(password) > 0) {
        RedisReplyPtr reply;
        if (username != nullptr && std::strlen(username) > 0) {
            reply.reset(static_cast<redisReply*>(redisCommand(
                context.get(), "AUTH %b %b", username, std::strlen(username),
                password, std::strlen(password))));
        } else {
            reply.reset(static_cast<redisReply*>(redisCommand(
                context.get(), "AUTH %b", password, std::strlen(password))));
        }
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
            return tl::make_unexpected(connection_error);
        }
    }

    if (db_index.value() != 0) {
        RedisReplyPtr reply(static_cast<redisReply*>(
            redisCommand(context.get(), "SELECT %d", db_index.value())));
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
            return tl::make_unexpected(connection_error);
        }
    }

    return context;
}

#endif

}  // namespace redis
}  // namespace common
}  // namespace ha
}  // namespace mooncake

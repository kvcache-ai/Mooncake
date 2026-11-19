#include "ha/common/redis/redis_connection.h"

#include <algorithm>
#include <chrono>
#include <exception>

#include "config/redis_connection_config.h"

#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#endif

namespace mooncake {
namespace ha {
namespace common {
namespace redis {

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
        if (text.find_first_not_of("0123456789") != std::string_view::npos)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
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
    const auto config = RedisConnectionConfig::FromEnvironment();
    if (!config.has_value()) {
        return tl::make_unexpected(config.error());
    }
    return config->db_index;
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

    if (connstring.empty() ||
        connstring.find_first_of("@, \t\r\n") != std::string_view::npos) {
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

uint16_t Crc16Xmodem(std::string_view data) {
    uint16_t crc = 0;
    for (unsigned char byte : data) {
        crc ^= static_cast<uint16_t>(byte) << 8;
        for (int bit = 0; bit < 8; ++bit) {
            crc = static_cast<uint16_t>((crc << 1) ^
                                        ((crc & 0x8000) ? 0x1021 : 0));
        }
    }
    return crc;
}

#ifndef STORE_USE_REDIS

tl::expected<RedisContextPtr, ErrorCode> ConnectRedis(
    std::string_view connstring, ErrorCode connection_error,
    std::chrono::steady_clock::time_point deadline) {
    (void)connstring;
    (void)connection_error;
    (void)deadline;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

#else

tl::expected<RedisContextPtr, ErrorCode> ConnectRedis(
    std::string_view connstring, ErrorCode connection_error,
    std::chrono::steady_clock::time_point deadline) {
    auto endpoint = ParseRedisEndpoint(connstring);
    if (!endpoint) {
        return tl::make_unexpected(endpoint.error());
    }

    const auto config = RedisConnectionConfig::FromEnvironment();
    if (!config.has_value()) {
        return tl::make_unexpected(config.error());
    }

    const auto remaining = [&](int configured_ms) {
        const int64_t ordinary = configured_ms;
        if (deadline == std::chrono::steady_clock::time_point::max())
            return ordinary;
        return std::min(ordinary,
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            deadline - std::chrono::steady_clock::now())
                            .count());
    };
    const auto ms = remaining(config->connect_timeout_ms);
    if (ms <= 0) return tl::make_unexpected(connection_error);
    timeval connect_timeout{static_cast<long>(ms / 1000),
                            static_cast<long>((ms % 1000) * 1000)};
    RedisContextPtr context(redisConnectWithTimeout(
        endpoint->host.c_str(), endpoint->port, connect_timeout));
    if (context == nullptr || context->err != 0) {
        return tl::make_unexpected(connection_error);
    }

    const auto command_ms = remaining(config->command_timeout_ms);
    if (command_ms <= 0) return tl::make_unexpected(connection_error);
    timeval command_timeout{static_cast<long>(command_ms / 1000),
                            static_cast<long>((command_ms % 1000) * 1000)};
    if (redisSetTimeout(context.get(), command_timeout) != REDIS_OK) {
        return tl::make_unexpected(connection_error);
    }

    if (!config->password.empty()) {
        RedisReplyPtr reply;
        if (!config->username.empty()) {
            reply.reset(static_cast<redisReply*>(redisCommand(
                context.get(), "AUTH %b %b", config->username.data(),
                config->username.size(), config->password.data(),
                config->password.size())));
        } else {
            reply.reset(static_cast<redisReply*>(
                redisCommand(context.get(), "AUTH %b", config->password.data(),
                             config->password.size())));
        }
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
            return tl::make_unexpected(connection_error);
        }
    }

    if (config->db_index != 0) {
        const auto select_ms = remaining(config->command_timeout_ms);
        if (select_ms <= 0) return tl::make_unexpected(connection_error);
        timeval select_timeout{static_cast<long>(select_ms / 1000),
                               static_cast<long>((select_ms % 1000) * 1000)};
        if (redisSetTimeout(context.get(), select_timeout) != REDIS_OK)
            return tl::make_unexpected(connection_error);
        RedisReplyPtr reply(static_cast<redisReply*>(
            redisCommand(context.get(), "SELECT %d", config->db_index)));
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

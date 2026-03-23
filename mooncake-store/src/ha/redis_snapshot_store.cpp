#include "ha/backends/redis/redis_snapshot_store.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string_view>

#include <glog/logging.h>
#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#endif

namespace mooncake {
namespace ha {
namespace backends {
namespace redis {

namespace {

constexpr std::string_view kSnapshotRoot = "mooncake_master_snapshot/";
constexpr std::string_view kSnapshotManifest = "manifest.txt";

bool IsDigit(char ch) { return std::isdigit(static_cast<unsigned char>(ch)); }

bool IsValidSnapshotId(std::string_view snapshot_id) {
    if (snapshot_id.size() != 19) {
        return false;
    }

    for (size_t i = 0; i < snapshot_id.size(); ++i) {
        if (i == 8 || i == 15) {
            if (snapshot_id[i] != '_') {
                return false;
            }
            continue;
        }

        if (!IsDigit(snapshot_id[i])) {
            return false;
        }
    }

    return true;
}

std::string TrimAsciiWhitespace(std::string value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front()))) {
        value.erase(value.begin());
    }
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back()))) {
        value.pop_back();
    }
    return value;
}

std::string BuildSnapshotPrefix(const SnapshotId& snapshot_id) {
    return std::string(kSnapshotRoot) + snapshot_id + "/";
}

std::string BuildManifestKey(const SnapshotId& snapshot_id) {
    return BuildSnapshotPrefix(snapshot_id) + std::string(kSnapshotManifest);
}

SnapshotDescriptor MakeSnapshotDescriptor(const SnapshotId& snapshot_id) {
    SnapshotDescriptor descriptor;
    descriptor.snapshot_id = snapshot_id;
    descriptor.manifest_key = BuildManifestKey(snapshot_id);
    descriptor.object_prefix = BuildSnapshotPrefix(snapshot_id);
    return descriptor;
}

tl::expected<long long, ErrorCode> ParseSnapshotScore(
    std::string_view snapshot_id) {
    if (!IsValidSnapshotId(snapshot_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::string digits;
    digits.reserve(snapshot_id.size() - 2);
    for (char ch : snapshot_id) {
        if (ch != '_') {
            digits.push_back(ch);
        }
    }

    try {
        return std::stoll(digits);
    } catch (const std::exception&) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
}

#ifdef STORE_USE_REDIS

constexpr auto kRedisDefaultPort = 6379;
constexpr auto kRedisConnectTimeout = std::chrono::seconds(3);
constexpr auto kRedisCommandTimeout = std::chrono::seconds(3);

constexpr char kPublishSnapshotScript[] = R"LUA(
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('SET', KEYS[2], ARGV[2])
return 1
)LUA";

constexpr char kDeleteSnapshotScript[] = R"LUA(
redis.call('ZREM', KEYS[1], ARGV[1])
local latest = redis.call('GET', KEYS[2])
if latest == ARGV[1] then
  local next = redis.call('ZREVRANGE', KEYS[1], 0, 0)
  if next[1] then
    redis.call('SET', KEYS[2], next[1])
  else
    redis.call('DEL', KEYS[2])
  end
end
return 1
)LUA";

struct RedisEndpoint {
    std::string host;
    int port = kRedisDefaultPort;
};

struct RedisContextDeleter {
    void operator()(redisContext* context) const {
        if (context != nullptr) {
            redisFree(context);
        }
    }
};

using RedisContextPtr = std::unique_ptr<redisContext, RedisContextDeleter>;

struct RedisReplyDeleter {
    void operator()(redisReply* reply) const {
        if (reply != nullptr) {
            freeReplyObject(reply);
        }
    }
};

using RedisReplyPtr = std::unique_ptr<redisReply, RedisReplyDeleter>;

bool IsStringReply(const redisReply* reply) {
    return reply != nullptr && (reply->type == REDIS_REPLY_STRING ||
                                reply->type == REDIS_REPLY_STATUS);
}

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

tl::expected<RedisContextPtr, ErrorCode> ConnectRedis(
    const std::string& connstring) {
    auto endpoint = ParseRedisEndpoint(connstring);
    if (!endpoint) {
        return tl::make_unexpected(endpoint.error());
    }

    timeval timeout{
        .tv_sec = static_cast<time_t>(kRedisConnectTimeout.count()),
        .tv_usec = 0,
    };
    RedisContextPtr context(redisConnectWithTimeout(endpoint->host.c_str(),
                                                    endpoint->port, timeout));
    if (context == nullptr || context->err != 0) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    timeval command_timeout{
        .tv_sec = static_cast<time_t>(kRedisCommandTimeout.count()),
        .tv_usec = 0,
    };
    if (redisSetTimeout(context.get(), command_timeout) != REDIS_OK) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    const char* password = std::getenv("MC_REDIS_PASSWORD");
    if (password != nullptr && std::strlen(password) > 0) {
        RedisReplyPtr auth_reply(static_cast<redisReply*>(redisCommand(
            context.get(), "AUTH %b", password, std::strlen(password))));
        if (auth_reply == nullptr || auth_reply->type == REDIS_REPLY_ERROR) {
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }
    }

    auto db_index = ResolveRedisDbIndex();
    if (!db_index) {
        return tl::make_unexpected(db_index.error());
    }
    if (db_index.value() != 0) {
        RedisReplyPtr select_reply(static_cast<redisReply*>(
            redisCommand(context.get(), "SELECT %d", db_index.value())));
        if (select_reply == nullptr ||
            select_reply->type == REDIS_REPLY_ERROR) {
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }
    }

    return context;
}

#endif  // STORE_USE_REDIS

}  // namespace

RedisSnapshotStore::RedisSnapshotStore(SerializerBackend* payload_backend,
                                       std::string connstring,
                                       ClusterNamespace cluster_namespace)
    : payload_backend_(payload_backend),
      connstring_(std::move(connstring)),
      cluster_namespace_(ResolveClusterNamespace(cluster_namespace)),
      latest_key_(BuildLatestKey(cluster_namespace_)),
      index_key_(BuildIndexKey(cluster_namespace_)) {}

#ifndef STORE_USE_REDIS

ErrorCode RedisSnapshotStore::Publish(const SnapshotDescriptor& snapshot) {
    (void)snapshot;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

tl::expected<std::optional<SnapshotDescriptor>, ErrorCode>
RedisSnapshotStore::GetLatest() {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

tl::expected<std::vector<SnapshotDescriptor>, ErrorCode>
RedisSnapshotStore::List(size_t limit) {
    (void)limit;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

ErrorCode RedisSnapshotStore::Delete(const SnapshotId& snapshot_id) {
    (void)snapshot_id;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

ClusterNamespace RedisSnapshotStore::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisSnapshotStore::BuildLatestKey(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisSnapshotStore::BuildIndexKey(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

#else

ErrorCode RedisSnapshotStore::Publish(const SnapshotDescriptor& snapshot) {
    if (!IsValidSnapshotId(snapshot.snapshot_id) || connstring_.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto score = ParseSnapshotScore(snapshot.snapshot_id);
    if (!score) {
        return score.error();
    }

    auto context = ConnectRedis(connstring_);
    if (!context) {
        return context.error();
    }

    RedisReplyPtr reply(static_cast<redisReply*>(redisCommand(
        context->get(), "EVAL %s 2 %b %b %lld %b", kPublishSnapshotScript,
        index_key_.data(), index_key_.size(), latest_key_.data(),
        latest_key_.size(), score.value(), snapshot.snapshot_id.data(),
        snapshot.snapshot_id.size())));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        return ErrorCode::PERSISTENT_FAIL;
    }

    return ErrorCode::OK;
}

tl::expected<std::optional<SnapshotDescriptor>, ErrorCode>
RedisSnapshotStore::GetLatest() {
    if (connstring_.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto context = ConnectRedis(connstring_);
    if (!context) {
        return tl::make_unexpected(context.error());
    }

    RedisReplyPtr reply(static_cast<redisReply*>(redisCommand(
        context->get(), "GET %b", latest_key_.data(), latest_key_.size())));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    if (reply->type == REDIS_REPLY_NIL) {
        return std::optional<SnapshotDescriptor>();
    }
    if (!IsStringReply(reply.get()) || reply->str == nullptr) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    auto latest_snapshot_id =
        TrimAsciiWhitespace(std::string(reply->str, reply->len));
    if (latest_snapshot_id.empty()) {
        return std::optional<SnapshotDescriptor>();
    }
    if (!IsValidSnapshotId(latest_snapshot_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return std::optional<SnapshotDescriptor>(
        MakeSnapshotDescriptor(latest_snapshot_id));
}

tl::expected<std::vector<SnapshotDescriptor>, ErrorCode>
RedisSnapshotStore::List(size_t limit) {
    if (connstring_.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto context = ConnectRedis(connstring_);
    if (!context) {
        return tl::make_unexpected(context.error());
    }

    const long long stop = limit == 0 ? -1 : static_cast<long long>(limit) - 1;
    RedisReplyPtr reply(static_cast<redisReply*>(
        redisCommand(context->get(), "ZREVRANGE %b %lld %lld",
                     index_key_.data(), index_key_.size(), 0LL, stop)));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    std::vector<SnapshotDescriptor> snapshots;
    snapshots.reserve(reply->elements);
    for (size_t i = 0; i < reply->elements; ++i) {
        const auto* element = reply->element[i];
        if (!IsStringReply(element) || element->str == nullptr) {
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }

        const std::string snapshot_id(element->str, element->len);
        if (!IsValidSnapshotId(snapshot_id)) {
            continue;
        }

        snapshots.emplace_back(MakeSnapshotDescriptor(snapshot_id));
    }

    return snapshots;
}

ErrorCode RedisSnapshotStore::Delete(const SnapshotId& snapshot_id) {
    if (!IsValidSnapshotId(snapshot_id) || connstring_.empty() ||
        payload_backend_ == nullptr) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto context = ConnectRedis(connstring_);
    if (!context) {
        return context.error();
    }

    RedisReplyPtr reply(static_cast<redisReply*>(redisCommand(
        context->get(), "EVAL %s 2 %b %b %b", kDeleteSnapshotScript,
        index_key_.data(), index_key_.size(), latest_key_.data(),
        latest_key_.size(), snapshot_id.data(), snapshot_id.size())));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        return ErrorCode::PERSISTENT_FAIL;
    }

    auto delete_result = payload_backend_->DeleteObjectsWithPrefix(
        BuildSnapshotPrefix(snapshot_id));
    if (!delete_result) {
        LOG(ERROR) << "Failed to delete snapshot payload after Redis catalog "
                      "update, snapshot_id="
                   << snapshot_id << ", error=" << delete_result.error();
        return ErrorCode::PERSISTENT_FAIL;
    }

    return ErrorCode::OK;
}

ClusterNamespace RedisSnapshotStore::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    if (cluster_namespace.empty()) {
        return "mooncake";
    }
    return cluster_namespace;
}

std::string RedisSnapshotStore::BuildLatestKey(
    const ClusterNamespace& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/snapshot/latest";
}

std::string RedisSnapshotStore::BuildIndexKey(
    const ClusterNamespace& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/snapshot/index";
}

#endif  // STORE_USE_REDIS

}  // namespace redis
}  // namespace backends
}  // namespace ha
}  // namespace mooncake

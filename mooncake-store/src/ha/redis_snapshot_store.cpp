#include "ha/backends/redis/redis_snapshot_store.h"

#include <cctype>
#include <exception>
#include <memory>
#include <optional>
#include <string_view>

#include <glog/logging.h>
#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#endif

#include "ha/backends/redis/redis_client_helper.h"

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

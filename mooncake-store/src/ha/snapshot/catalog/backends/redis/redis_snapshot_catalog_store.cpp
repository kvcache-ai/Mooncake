#include "ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.h"

#include <exception>
#include <memory>
#include <optional>
#include <string_view>

#include <glog/logging.h>
#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#endif

#include "ha/common/redis/redis_connection.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace redis {

namespace {

using common::redis::ConnectRedis;
using common::redis::IsStringReply;
using common::redis::RedisReplyPtr;
using common::redis::SanitizeHashTagComponent;

tl::expected<long long, ErrorCode> ParseSnapshotScore(
    std::string_view snapshot_id) {
    if (!snapshot_catalog_store_detail::IsValidSnapshotId(snapshot_id)) {
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

RedisSnapshotCatalogStore::RedisSnapshotCatalogStore(
    SnapshotObjectStore* object_store, std::string connstring,
    ClusterNamespace cluster_namespace)
    : object_store_(object_store),
      connstring_(std::move(connstring)),
      cluster_namespace_(ResolveClusterNamespace(cluster_namespace)),
      latest_key_(BuildLatestKey(cluster_namespace_)),
      index_key_(BuildIndexKey(cluster_namespace_)) {}

#ifndef STORE_USE_REDIS

ErrorCode RedisSnapshotCatalogStore::Publish(
    const SnapshotDescriptor& snapshot) {
    (void)snapshot;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

tl::expected<std::optional<SnapshotDescriptor>, ErrorCode>
RedisSnapshotCatalogStore::GetLatest() {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

tl::expected<std::vector<SnapshotDescriptor>, ErrorCode>
RedisSnapshotCatalogStore::List(size_t limit) {
    (void)limit;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

ErrorCode RedisSnapshotCatalogStore::Delete(const SnapshotId& snapshot_id) {
    (void)snapshot_id;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

ClusterNamespace RedisSnapshotCatalogStore::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisSnapshotCatalogStore::BuildLatestKey(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisSnapshotCatalogStore::BuildIndexKey(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

#else

ErrorCode RedisSnapshotCatalogStore::Publish(
    const SnapshotDescriptor& snapshot) {
    if (!snapshot_catalog_store_detail::IsValidSnapshotId(
            snapshot.snapshot_id) ||
        connstring_.empty()) {
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
RedisSnapshotCatalogStore::GetLatest() {
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
        snapshot_catalog_store_detail::TrimAsciiWhitespace(
            std::string(reply->str, reply->len));
    if (latest_snapshot_id.empty()) {
        return std::optional<SnapshotDescriptor>();
    }
    if (!snapshot_catalog_store_detail::IsValidSnapshotId(latest_snapshot_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return std::optional<SnapshotDescriptor>(
        snapshot_catalog_store_detail::MakeSnapshotDescriptor(
            latest_snapshot_id));
}

tl::expected<std::vector<SnapshotDescriptor>, ErrorCode>
RedisSnapshotCatalogStore::List(size_t limit) {
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
        if (!snapshot_catalog_store_detail::IsValidSnapshotId(snapshot_id)) {
            continue;
        }

        snapshots.emplace_back(
            snapshot_catalog_store_detail::MakeSnapshotDescriptor(snapshot_id));
    }

    return snapshots;
}

ErrorCode RedisSnapshotCatalogStore::Delete(const SnapshotId& snapshot_id) {
    if (!snapshot_catalog_store_detail::IsValidSnapshotId(snapshot_id) ||
        connstring_.empty() || object_store_ == nullptr) {
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

    auto delete_result = object_store_->DeleteObjectsWithPrefix(
        snapshot_catalog_store_detail::BuildSnapshotPrefix(snapshot_id));
    if (!delete_result) {
        LOG(ERROR) << "Failed to delete snapshot objects after Redis catalog "
                      "update, snapshot_id="
                   << snapshot_id << ", error=" << delete_result.error();
        return ErrorCode::PERSISTENT_FAIL;
    }

    return ErrorCode::OK;
}

ClusterNamespace RedisSnapshotCatalogStore::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    if (cluster_namespace.empty()) {
        return "mooncake";
    }
    return cluster_namespace;
}

std::string RedisSnapshotCatalogStore::BuildLatestKey(
    const ClusterNamespace& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/snapshot/latest";
}

std::string RedisSnapshotCatalogStore::BuildIndexKey(
    const ClusterNamespace& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/snapshot/index";
}

#endif  // STORE_USE_REDIS

}  // namespace redis
}  // namespace backends
}  // namespace ha
}  // namespace mooncake

#include "ha/progress/backends/redis/redis_standby_progress_store.h"

#include <utility>

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

}  // namespace

RedisStandbyProgressStore::RedisStandbyProgressStore(
    std::string connstring, ClusterNamespace cluster_namespace)
    : connstring_(std::move(connstring)),
      cluster_namespace_(ResolveClusterNamespace(cluster_namespace)),
      index_key_(BuildIndexKey(cluster_namespace_)) {}

#ifndef STORE_USE_REDIS

ErrorCode RedisStandbyProgressStore::Publish(const StandbyProgressRecord&) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

tl::expected<std::vector<StandbyProgressRecord>, ErrorCode>
RedisStandbyProgressStore::List() {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

ErrorCode RedisStandbyProgressStore::Delete(std::string_view) {
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

ClusterNamespace RedisStandbyProgressStore::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisStandbyProgressStore::BuildIndexKey(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisStandbyProgressStore::BuildRecordKey(
    const ClusterNamespace& cluster_namespace, std::string_view standby_id) {
    (void)standby_id;
    return cluster_namespace;
}

#else

ErrorCode RedisStandbyProgressStore::Publish(
    const StandbyProgressRecord& record) {
    if (connstring_.empty() || record.standby_id.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto context = ConnectRedis(connstring_);
    if (!context) {
        return context.error();
    }

    const auto value =
        standby_progress_store_detail::SerializeStandbyProgressValue(record);
    const auto record_key =
        BuildRecordKey(cluster_namespace_, record.standby_id);
    const auto ttl = static_cast<long long>(
        standby_progress_store_detail::kStandbyProgressRedisTtl.count());

    RedisReplyPtr index_reply(static_cast<redisReply*>(redisCommand(
        context->get(), "SADD %b %b", index_key_.data(), index_key_.size(),
        record.standby_id.data(), record.standby_id.size())));
    if (index_reply == nullptr || index_reply->type == REDIS_REPLY_ERROR) {
        return ErrorCode::PERSISTENT_FAIL;
    }

    RedisReplyPtr set_reply(static_cast<redisReply*>(
        redisCommand(context->get(), "SET %b %b EX %lld", record_key.data(),
                     record_key.size(), value.data(), value.size(), ttl)));
    if (set_reply == nullptr || set_reply->type == REDIS_REPLY_ERROR) {
        return ErrorCode::PERSISTENT_FAIL;
    }
    return ErrorCode::OK;
}

tl::expected<std::vector<StandbyProgressRecord>, ErrorCode>
RedisStandbyProgressStore::List() {
    if (connstring_.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto context = ConnectRedis(connstring_);
    if (!context) {
        return tl::make_unexpected(context.error());
    }

    RedisReplyPtr index_reply(static_cast<redisReply*>(redisCommand(
        context->get(), "SMEMBERS %b", index_key_.data(), index_key_.size())));
    if (index_reply == nullptr || index_reply->type == REDIS_REPLY_ERROR) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    if (index_reply->type != REDIS_REPLY_ARRAY) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    std::vector<StandbyProgressRecord> records;
    records.reserve(index_reply->elements);
    for (size_t i = 0; i < index_reply->elements; ++i) {
        const auto* member = index_reply->element[i];
        if (!IsStringReply(member) || member->str == nullptr) {
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }

        std::string_view standby_id(member->str, member->len);
        const auto record_key = BuildRecordKey(cluster_namespace_, standby_id);
        RedisReplyPtr value_reply(static_cast<redisReply*>(redisCommand(
            context->get(), "GET %b", record_key.data(), record_key.size())));
        if (value_reply == nullptr || value_reply->type == REDIS_REPLY_ERROR) {
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }
        if (value_reply->type == REDIS_REPLY_NIL) {
            RedisReplyPtr cleanup_reply(static_cast<redisReply*>(redisCommand(
                context->get(), "SREM %b %b", index_key_.data(),
                index_key_.size(), standby_id.data(), standby_id.size())));
            if (cleanup_reply == nullptr ||
                cleanup_reply->type == REDIS_REPLY_ERROR) {
                LOG(WARNING)
                    << "Failed to cleanup stale Redis standby progress "
                    << "index entry, standby_id=" << std::string(standby_id);
            }
            continue;
        }
        if (!IsStringReply(value_reply.get()) || value_reply->str == nullptr) {
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }

        auto record =
            standby_progress_store_detail::DeserializeStandbyProgressValue(
                standby_id,
                std::string_view(value_reply->str, value_reply->len));
        if (!record) {
            return tl::make_unexpected(record.error());
        }
        records.push_back(std::move(record.value()));
    }

    return records;
}

ErrorCode RedisStandbyProgressStore::Delete(std::string_view standby_id) {
    if (connstring_.empty() || standby_id.empty()) {
        return ErrorCode::OK;
    }

    auto context = ConnectRedis(connstring_);
    if (!context) {
        return context.error();
    }

    const auto record_key = BuildRecordKey(cluster_namespace_, standby_id);
    RedisReplyPtr index_reply(static_cast<redisReply*>(
        redisCommand(context->get(), "SREM %b %b", index_key_.data(),
                     index_key_.size(), standby_id.data(), standby_id.size())));
    if (index_reply == nullptr || index_reply->type == REDIS_REPLY_ERROR) {
        return ErrorCode::PERSISTENT_FAIL;
    }

    RedisReplyPtr del_reply(static_cast<redisReply*>(redisCommand(
        context->get(), "DEL %b", record_key.data(), record_key.size())));
    if (del_reply == nullptr || del_reply->type == REDIS_REPLY_ERROR) {
        return ErrorCode::PERSISTENT_FAIL;
    }
    return ErrorCode::OK;
}

ClusterNamespace RedisStandbyProgressStore::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    if (cluster_namespace.empty()) {
        return "mooncake";
    }
    return cluster_namespace;
}

std::string RedisStandbyProgressStore::BuildIndexKey(
    const ClusterNamespace& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/standby_progress/index";
}

std::string RedisStandbyProgressStore::BuildRecordKey(
    const ClusterNamespace& cluster_namespace, std::string_view standby_id) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/standby_progress/record/" +
           std::string(standby_id);
}

#endif

}  // namespace redis
}  // namespace backends
}  // namespace ha
}  // namespace mooncake

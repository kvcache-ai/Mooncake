#include "ha/oplog/backends/redis/redis_oplog_store.h"

#include <chrono>
#include <exception>
#include <iomanip>
#include <sstream>
#include <thread>
#include <utility>

#include <glog/logging.h>
#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#endif

#include "ha/common/redis/redis_connection.h"
#include "ha/oplog/oplog_codec.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace redis {

namespace {

using common::redis::ConnectRedis;
using common::redis::IsStringReply;
using common::redis::RedisReplyPtr;
using common::redis::SanitizeHashTagComponent;

constexpr auto kPollRetrySleep = std::chrono::milliseconds(50);

#ifdef STORE_USE_REDIS

constexpr char kAppendOpLogScript[] = R"LUA(
redis.call('SET', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[3])
local latest = tonumber(redis.call('GET', KEYS[3]) or '0')
local seq = tonumber(ARGV[1])
if seq > latest then
  redis.call('SET', KEYS[3], ARGV[1])
end
return ARGV[1]
)LUA";

#endif

}  // namespace

RedisOpLogStore::RedisOpLogStore(std::string connstring,
                                 ClusterNamespace cluster_namespace)
    : connstring_(std::move(connstring)),
      cluster_namespace_(ResolveClusterNamespace(cluster_namespace)),
      latest_key_(BuildLatestKey(cluster_namespace_)),
      index_key_(BuildIndexKey(cluster_namespace_)) {}

#ifndef STORE_USE_REDIS

tl::expected<OpLogSequenceId, ErrorCode> RedisOpLogStore::Append(
    const OpLogAppendRequest& request) {
    (void)request;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

tl::expected<OpLogPollResult, ErrorCode> RedisOpLogStore::PollFrom(
    OpLogSequenceId start_seq, size_t max_records,
    std::chrono::milliseconds timeout) {
    (void)start_seq;
    (void)max_records;
    (void)timeout;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

tl::expected<OpLogSequenceId, ErrorCode> RedisOpLogStore::GetLatestSequence() {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

ClusterNamespace RedisOpLogStore::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisOpLogStore::FormatSequence(OpLogSequenceId seq) {
    return std::to_string(seq);
}

std::string RedisOpLogStore::BuildLatestKey(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisOpLogStore::BuildIndexKey(
    const ClusterNamespace& cluster_namespace) {
    return cluster_namespace;
}

std::string RedisOpLogStore::BuildEntryKey(
    const ClusterNamespace& cluster_namespace, OpLogSequenceId seq) {
    (void)seq;
    return cluster_namespace;
}

tl::expected<OpLogSequenceId, ErrorCode> RedisOpLogStore::ParseSequenceMember(
    std::string_view member) {
    (void)member;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

#else

tl::expected<OpLogSequenceId, ErrorCode> RedisOpLogStore::Append(
    const OpLogAppendRequest& request) {
    if (connstring_.empty() || request.expected_next_seq == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto decoded = oplog::DeserializeEntryPayload(request.payload);
    if (!decoded) {
        return tl::make_unexpected(decoded.error());
    }
    if (decoded->sequence_id != request.expected_next_seq) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto context = ConnectRedis(connstring_);
    if (!context) {
        return tl::make_unexpected(context.error());
    }

    const auto sequence_text = FormatSequence(request.expected_next_seq);
    const auto entry_key =
        BuildEntryKey(cluster_namespace_, request.expected_next_seq);
    RedisReplyPtr reply(static_cast<redisReply*>(redisCommand(
        context->get(), "EVAL %s 3 %b %b %b %b %b %b", kAppendOpLogScript,
        entry_key.data(), entry_key.size(), index_key_.data(),
        index_key_.size(), latest_key_.data(), latest_key_.size(),
        sequence_text.data(), sequence_text.size(), request.payload.data(),
        request.payload.size(), sequence_text.data(), sequence_text.size())));
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    return request.expected_next_seq;
}

tl::expected<OpLogPollResult, ErrorCode> RedisOpLogStore::PollFrom(
    OpLogSequenceId start_seq, size_t max_records,
    std::chrono::milliseconds timeout) {
    if (connstring_.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    OpLogPollResult result;
    result.next_seq = start_seq + 1;

    for (;;) {
        auto context = ConnectRedis(connstring_);
        if (!context) {
            return tl::make_unexpected(context.error());
        }

        RedisReplyPtr reply(static_cast<redisReply*>(redisCommand(
            context->get(), "ZRANGEBYSCORE %b %llu +inf LIMIT 0 %llu",
            index_key_.data(), index_key_.size(),
            static_cast<unsigned long long>(start_seq + 1),
            static_cast<unsigned long long>(max_records))));
        if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }

        if (reply->type != REDIS_REPLY_ARRAY) {
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }

        if (reply->elements > 0) {
            result.records.reserve(reply->elements);
            for (size_t i = 0; i < reply->elements; ++i) {
                const auto* member = reply->element[i];
                if (!IsStringReply(member) || member->str == nullptr) {
                    return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
                }

                auto seq = ParseSequenceMember(
                    std::string_view(member->str, member->len));
                if (!seq) {
                    return tl::make_unexpected(seq.error());
                }

                const auto entry_key = BuildEntryKey(cluster_namespace_, *seq);
                RedisReplyPtr value_reply(static_cast<redisReply*>(
                    redisCommand(context->get(), "GET %b", entry_key.data(),
                                 entry_key.size())));
                if (value_reply == nullptr ||
                    value_reply->type == REDIS_REPLY_ERROR) {
                    return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
                }
                if (!IsStringReply(value_reply.get()) ||
                    value_reply->str == nullptr) {
                    return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
                }

                result.records.push_back(OpLogRecord{
                    .seq = *seq,
                    .producer_view_version = 0,
                    .payload = std::string(value_reply->str, value_reply->len),
                });
            }

            result.next_seq = result.records.back().seq + 1;
            result.timed_out = false;
            return result;
        }

        if (timeout.count() == 0 ||
            std::chrono::steady_clock::now() >= deadline) {
            result.timed_out = true;
            return result;
        }

        std::this_thread::sleep_for(kPollRetrySleep);
    }
}

tl::expected<OpLogSequenceId, ErrorCode> RedisOpLogStore::GetLatestSequence() {
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
        return OpLogSequenceId{0};
    }
    if (!IsStringReply(reply.get()) || reply->str == nullptr) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    auto sequence =
        ParseSequenceMember(std::string_view(reply->str, reply->len));
    if (!sequence) {
        return tl::make_unexpected(sequence.error());
    }
    return sequence.value();
}

ClusterNamespace RedisOpLogStore::ResolveClusterNamespace(
    const ClusterNamespace& cluster_namespace) {
    if (cluster_namespace.empty()) {
        return "mooncake";
    }
    return cluster_namespace;
}

std::string RedisOpLogStore::FormatSequence(OpLogSequenceId seq) {
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0')
        << static_cast<unsigned long long>(seq);
    return oss.str();
}

std::string RedisOpLogStore::BuildLatestKey(
    const ClusterNamespace& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/oplog/latest";
}

std::string RedisOpLogStore::BuildIndexKey(
    const ClusterNamespace& cluster_namespace) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/oplog/index";
}

std::string RedisOpLogStore::BuildEntryKey(
    const ClusterNamespace& cluster_namespace, OpLogSequenceId seq) {
    const auto hash_tag = SanitizeHashTagComponent(cluster_namespace);
    return "mooncake-store/{" + hash_tag + "}/oplog/entry/" +
           FormatSequence(seq);
}

tl::expected<OpLogSequenceId, ErrorCode> RedisOpLogStore::ParseSequenceMember(
    std::string_view member) {
    try {
        return static_cast<OpLogSequenceId>(std::stoull(std::string(member)));
    } catch (const std::exception&) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
}

#endif

}  // namespace redis
}  // namespace backends
}  // namespace ha
}  // namespace mooncake

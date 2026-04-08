#include "ha/oplog/backends/redis/redis_oplog_store.h"

#include <chrono>
#include <exception>
#include <iomanip>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

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

thread_local std::unordered_map<std::string, common::redis::RedisContextPtr>
    g_redis_context_cache;

tl::expected<redisContext*, ErrorCode> AcquireCachedRedisContext(
    std::string_view connstring,
    ErrorCode connection_error = ErrorCode::PERSISTENT_FAIL) {
    auto& context = g_redis_context_cache[std::string(connstring)];
    if (context != nullptr && context->err == 0) {
        return context.get();
    }

    auto connected = ConnectRedis(connstring, connection_error);
    if (!connected) {
        g_redis_context_cache.erase(std::string(connstring));
        return tl::make_unexpected(connected.error());
    }

    context = std::move(connected.value());
    return context.get();
}

void ResetCachedRedisContext(std::string_view connstring) {
    g_redis_context_cache.erase(std::string(connstring));
}

#ifdef STORE_USE_REDIS

RedisReplyPtr ExecuteRedisCommandArgv(redisContext* context,
                                      const std::vector<std::string>& args) {
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    argv.reserve(args.size());
    argvlen.reserve(args.size());
    for (const auto& arg : args) {
        argv.push_back(arg.data());
        argvlen.push_back(arg.size());
    }
    return RedisReplyPtr(static_cast<redisReply*>(redisCommandArgv(
        context, static_cast<int>(argv.size()), argv.data(), argvlen.data())));
}

#endif

std::string DescribeRedisCommandFailure(redisContext* context,
                                        const redisReply* reply) {
    if (reply != nullptr && reply->type == REDIS_REPLY_ERROR &&
        reply->str != nullptr) {
        return reply->str;
    }
    if (context != nullptr && context->err != 0) {
        return context->errstr;
    }
    return "unknown";
}

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

ErrorCode RedisOpLogStore::CleanupBefore(OpLogSequenceId before_sequence_id) {
    (void)before_sequence_id;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
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

    const auto sequence_text = FormatSequence(request.expected_next_seq);
    const auto entry_key =
        BuildEntryKey(cluster_namespace_, request.expected_next_seq);

    auto execute_append = [&]() -> tl::expected<OpLogSequenceId, ErrorCode> {
        auto context = AcquireCachedRedisContext(connstring_);
        if (!context) {
            return tl::make_unexpected(context.error());
        }

        RedisReplyPtr reply(static_cast<redisReply*>(redisCommand(
            context.value(), "EVAL %s 3 %b %b %b %b %b %b", kAppendOpLogScript,
            entry_key.data(), entry_key.size(), index_key_.data(),
            index_key_.size(), latest_key_.data(), latest_key_.size(),
            sequence_text.data(), sequence_text.size(), request.payload.data(),
            request.payload.size(), sequence_text.data(),
            sequence_text.size())));
        if (reply == nullptr) {
            LOG(WARNING) << "Redis OpLog append failed, sequence_id="
                         << request.expected_next_seq << ", error="
                         << DescribeRedisCommandFailure(context.value(),
                                                        reply.get());
            ResetCachedRedisContext(connstring_);
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }
        if (reply->type == REDIS_REPLY_ERROR) {
            LOG(WARNING) << "Redis OpLog append returned error, sequence_id="
                         << request.expected_next_seq << ", error="
                         << DescribeRedisCommandFailure(context.value(),
                                                        reply.get());
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }

        return request.expected_next_seq;
    };

    auto append_result = execute_append();
    if (!append_result) {
        append_result = execute_append();
        if (!append_result) {
            return tl::make_unexpected(append_result.error());
        }
    }

    return append_result;
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
        auto context = AcquireCachedRedisContext(connstring_);
        if (!context) {
            return tl::make_unexpected(context.error());
        }

        RedisReplyPtr reply(static_cast<redisReply*>(redisCommand(
            context.value(), "ZRANGEBYSCORE %b %llu +inf LIMIT 0 %llu",
            index_key_.data(), index_key_.size(),
            static_cast<unsigned long long>(start_seq + 1),
            static_cast<unsigned long long>(max_records))));
        if (reply == nullptr) {
            LOG(WARNING) << "Redis OpLog poll failed, start_seq=" << start_seq
                         << ", error="
                         << DescribeRedisCommandFailure(context.value(),
                                                        reply.get());
            ResetCachedRedisContext(connstring_);
            return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
        }
        if (reply->type == REDIS_REPLY_ERROR) {
            LOG(WARNING) << "Redis OpLog poll returned error, start_seq="
                         << start_seq << ", error="
                         << DescribeRedisCommandFailure(context.value(),
                                                        reply.get());
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
                    redisCommand(context.value(), "GET %b", entry_key.data(),
                                 entry_key.size())));
                if (value_reply == nullptr) {
                    LOG(WARNING)
                        << "Redis OpLog GET entry failed, sequence_id=" << *seq
                        << ", error="
                        << DescribeRedisCommandFailure(context.value(),
                                                       value_reply.get());
                    ResetCachedRedisContext(connstring_);
                    return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
                }
                if (value_reply->type == REDIS_REPLY_ERROR) {
                    LOG(WARNING)
                        << "Redis OpLog GET entry returned error, sequence_id="
                        << *seq << ", error="
                        << DescribeRedisCommandFailure(context.value(),
                                                       value_reply.get());
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

    auto context = AcquireCachedRedisContext(connstring_);
    if (!context) {
        return tl::make_unexpected(context.error());
    }

    RedisReplyPtr reply(static_cast<redisReply*>(redisCommand(
        context.value(), "GET %b", latest_key_.data(), latest_key_.size())));
    if (reply == nullptr) {
        LOG(WARNING) << "Redis OpLog latest-seq lookup failed, error="
                     << DescribeRedisCommandFailure(context.value(),
                                                    reply.get());
        ResetCachedRedisContext(connstring_);
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        LOG(WARNING) << "Redis OpLog latest-seq lookup returned error: "
                     << DescribeRedisCommandFailure(context.value(),
                                                    reply.get());
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

ErrorCode RedisOpLogStore::CleanupBefore(OpLogSequenceId before_sequence_id) {
    if (connstring_.empty() || before_sequence_id == 0) {
        return ErrorCode::OK;
    }

    constexpr size_t kCleanupBatchSize = 1024;
    const auto max_score =
        static_cast<unsigned long long>(before_sequence_id - 1);

    auto cleanup_once = [&]() -> ErrorCode {
        for (;;) {
            auto context = AcquireCachedRedisContext(connstring_);
            if (!context) {
                return context.error();
            }

            RedisReplyPtr range_reply(static_cast<redisReply*>(redisCommand(
                context.value(), "ZRANGEBYSCORE %b -inf %llu LIMIT 0 %llu",
                index_key_.data(), index_key_.size(), max_score,
                static_cast<unsigned long long>(kCleanupBatchSize))));
            if (range_reply == nullptr) {
                LOG(WARNING)
                    << "Redis OpLog cleanup range query failed, "
                    << "before_sequence_id=" << before_sequence_id << ", error="
                    << DescribeRedisCommandFailure(context.value(),
                                                   range_reply.get());
                ResetCachedRedisContext(connstring_);
                return ErrorCode::PERSISTENT_FAIL;
            }
            if (range_reply->type == REDIS_REPLY_ERROR) {
                LOG(WARNING) << "Redis OpLog cleanup range query returned "
                                "error, before_sequence_id="
                             << before_sequence_id << ", error="
                             << DescribeRedisCommandFailure(context.value(),
                                                            range_reply.get());
                return ErrorCode::PERSISTENT_FAIL;
            }
            if (range_reply->type != REDIS_REPLY_ARRAY) {
                return ErrorCode::PERSISTENT_FAIL;
            }
            if (range_reply->elements == 0) {
                return ErrorCode::OK;
            }

            std::vector<std::string> members;
            members.reserve(range_reply->elements);
            std::vector<std::string> entry_keys;
            entry_keys.reserve(range_reply->elements);
            for (size_t i = 0; i < range_reply->elements; ++i) {
                const auto* member = range_reply->element[i];
                if (!IsStringReply(member) || member->str == nullptr) {
                    return ErrorCode::PERSISTENT_FAIL;
                }

                std::string member_text(member->str, member->len);
                auto seq = ParseSequenceMember(member_text);
                if (!seq) {
                    return seq.error();
                }

                members.push_back(std::move(member_text));
                entry_keys.push_back(BuildEntryKey(cluster_namespace_, *seq));
            }

            std::vector<std::string> zrem_args;
            zrem_args.reserve(2 + members.size());
            zrem_args.push_back("ZREM");
            zrem_args.push_back(index_key_);
            zrem_args.insert(zrem_args.end(), members.begin(), members.end());
            RedisReplyPtr zrem_reply =
                ExecuteRedisCommandArgv(context.value(), zrem_args);
            if (zrem_reply == nullptr) {
                LOG(WARNING)
                    << "Redis OpLog cleanup ZREM failed, "
                    << "before_sequence_id=" << before_sequence_id << ", error="
                    << DescribeRedisCommandFailure(context.value(),
                                                   zrem_reply.get());
                ResetCachedRedisContext(connstring_);
                return ErrorCode::PERSISTENT_FAIL;
            }
            if (zrem_reply->type == REDIS_REPLY_ERROR) {
                LOG(WARNING)
                    << "Redis OpLog cleanup ZREM returned error, "
                    << "before_sequence_id=" << before_sequence_id << ", error="
                    << DescribeRedisCommandFailure(context.value(),
                                                   zrem_reply.get());
                return ErrorCode::PERSISTENT_FAIL;
            }

            std::vector<std::string> del_args;
            del_args.reserve(1 + entry_keys.size());
            del_args.push_back("DEL");
            del_args.insert(del_args.end(), entry_keys.begin(),
                            entry_keys.end());
            RedisReplyPtr del_reply =
                ExecuteRedisCommandArgv(context.value(), del_args);
            if (del_reply == nullptr) {
                LOG(WARNING)
                    << "Redis OpLog cleanup DEL failed, "
                    << "before_sequence_id=" << before_sequence_id << ", error="
                    << DescribeRedisCommandFailure(context.value(),
                                                   del_reply.get());
                ResetCachedRedisContext(connstring_);
                return ErrorCode::PERSISTENT_FAIL;
            }
            if (del_reply->type == REDIS_REPLY_ERROR) {
                LOG(WARNING)
                    << "Redis OpLog cleanup DEL returned error, "
                    << "before_sequence_id=" << before_sequence_id << ", error="
                    << DescribeRedisCommandFailure(context.value(),
                                                   del_reply.get());
                return ErrorCode::PERSISTENT_FAIL;
            }

            if (range_reply->elements < kCleanupBatchSize) {
                return ErrorCode::OK;
            }
        }
    };

    auto err = cleanup_once();
    if (err == ErrorCode::OK) {
        return ErrorCode::OK;
    }

    err = cleanup_once();
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Redis OpLog cleanup failed after retry, "
                     << "before_sequence_id=" << before_sequence_id
                     << ", error=" << toString(err);
    }
    return err;
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

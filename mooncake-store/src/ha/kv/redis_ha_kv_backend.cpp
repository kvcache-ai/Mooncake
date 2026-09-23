#include "ha/kv/redis_ha_kv_backend.h"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "types.h"

#ifdef STORE_USE_REDIS
#include <hiredis/hiredis.h>
#endif

namespace mooncake {
namespace {

constexpr char kPutScript[] = R"LUA(
redis.call('SET', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[1], 0, ARGV[2])
return 1
)LUA";

constexpr char kTxnScript[] = R"LUA(
local ncomp = tonumber(ARGV[1])
local nputs = tonumber(ARGV[2])
local i = 3
for _ = 1, ncomp do
  local slot = tonumber(ARGV[i])
  i = i + 1
  local kind = ARGV[i]
  i = i + 1
  local expected = ARGV[i]
  i = i + 1
  local current = redis.call('GET', KEYS[slot])
  if kind == 'not_exists' then
    if current then
      return 0
    end
  elseif kind == 'value_equals' then
    if (not current) or current ~= expected then
      return 0
    end
  else
    return -1
  end
end
for _ = 1, nputs do
  local slot = tonumber(ARGV[i])
  i = i + 1
  local value = ARGV[i]
  i = i + 1
  local logical = ARGV[i]
  i = i + 1
  redis.call('SET', KEYS[slot], value)
  redis.call('ZADD', KEYS[1], 0, logical)
end
return 1
)LUA";

constexpr char kRangeScript[] = R"LUA(
local limit = tonumber(ARGV[3])
local members
if limit == 0 then
  members = redis.call('ZRANGEBYLEX', KEYS[1], ARGV[1], ARGV[2])
else
  members = redis.call('ZRANGEBYLEX', KEYS[1], ARGV[1], ARGV[2],
                       'LIMIT', '0', ARGV[3])
end
local logical_prefix = ARGV[4]
local redis_prefix = ARGV[5]
local out = {}
for _, logical in ipairs(members) do
  if string.sub(logical, 1, #logical_prefix) == logical_prefix then
    local rest = string.sub(logical, #logical_prefix + 1)
    local value = redis.call('GET', redis_prefix .. rest)
    if value then
      table.insert(out, logical)
      table.insert(out, value)
    end
  end
end
return out
)LUA";

constexpr char kDeleteRangeScript[] = R"LUA(
local members = redis.call('ZRANGEBYLEX', KEYS[1], ARGV[1], ARGV[2])
local logical_prefix = ARGV[3]
local redis_prefix = ARGV[4]
for _, logical in ipairs(members) do
  if string.sub(logical, 1, #logical_prefix) == logical_prefix then
    local rest = string.sub(logical, #logical_prefix + 1)
    redis.call('DEL', redis_prefix .. rest)
  end
  redis.call('ZREM', KEYS[1], logical)
end
return 1
)LUA";

struct MappedOpLogKey {
    std::string cluster_id;
    std::string logical_prefix;
    std::string redis_prefix;
    std::string redis_key;
    std::string index_key;
};

bool IsFiniteBound(std::string_view key) {
    return !key.empty() && key != std::string_view("\0", 1);
}

tl::expected<MappedOpLogKey, ErrorCode> MapOpLogKey(std::string_view logical) {
    constexpr std::string_view kRoot = "/oplog/";
    if (logical.size() <= kRoot.size() ||
        logical.substr(0, kRoot.size()) != kRoot) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto body = logical.substr(kRoot.size());
    const auto slash = body.find('/');
    if (slash == std::string_view::npos || slash == 0 ||
        slash + 1 >= body.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const std::string cluster_id(body.substr(0, slash));
    if (!IsValidClusterIdComponent(cluster_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto rest = body.substr(slash + 1);
    const auto tag = ha::common::redis::SanitizeHashTagComponent(cluster_id);
    MappedOpLogKey mapped;
    mapped.cluster_id = cluster_id;
    mapped.logical_prefix = "/oplog/" + cluster_id + "/";
    mapped.redis_prefix = "mooncake-store/{" + tag + "}/oplog/";
    mapped.redis_key = mapped.redis_prefix + std::string(rest);
    mapped.index_key = "mooncake-store/{" + tag + "}/oplog-index";
    return mapped;
}

ErrorCode ValidateRangeBounds(std::string_view begin_key,
                              std::string_view end_key) {
    if (!IsFiniteBound(begin_key) || !IsFiniteBound(end_key) ||
        begin_key > end_key) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

#ifdef STORE_USE_REDIS

using ha::common::redis::RedisReplyPtr;

std::string ReplyBytes(const redisReply* reply) {
    if (reply == nullptr || reply->str == nullptr) {
        return {};
    }
    return std::string(reply->str, static_cast<size_t>(reply->len));
}

RedisReplyPtr Exec(redisContext* context,
                    const std::vector<std::string>& args) {
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    argv.reserve(args.size());
    argvlen.reserve(args.size());
    for (const auto& arg : args) {
        argv.push_back(arg.data());
        argvlen.push_back(arg.size());
    }
    auto* raw = static_cast<redisReply*>(redisCommandArgv(
        context, static_cast<int>(argv.size()), argv.data(), argvlen.data()));
    return RedisReplyPtr(raw);
}

#endif

}  // namespace

ErrorCode RedisHaKvBackend::Connect(std::string connstring) {
    std::lock_guard<std::mutex> lock(mu_);
    connstring_ = std::move(connstring);
    context_.reset();
    return EnsureConnectedLocked();
}

ErrorCode RedisHaKvBackend::EnsureConnectedLocked() {
#ifndef STORE_USE_REDIS
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#else
    if (connstring_.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (context_ != nullptr && context_->err == 0) {
        return ErrorCode::OK;
    }
    context_.reset();
    auto connected = ha::common::redis::ConnectRedis(
        connstring_, ErrorCode::ETCD_OPERATION_ERROR);
    if (!connected) {
        return connected.error();
    }
    context_ = std::move(connected.value());
    return ErrorCode::OK;
#endif
}

bool RedisHaKvBackend::SupportsTxn() const { return true; }

ErrorCode RedisHaKvBackend::Get(std::string_view key, std::string& value) {
    auto mapped = MapOpLogKey(key);
    if (!mapped) {
        return mapped.error();
    }
    std::lock_guard<std::mutex> lock(mu_);
    const ErrorCode connected = EnsureConnectedLocked();
    if (connected != ErrorCode::OK) {
        return connected;
    }
#ifdef STORE_USE_REDIS
    auto reply = Exec(context_.get(), {"GET", mapped->redis_key});
    if (reply == nullptr) {
        LOG(WARNING) << "Redis GET failed for OpLog key";
        context_.reset();
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        return ErrorCode::ETCD_KEY_NOT_EXIST;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        LOG(WARNING) << "Redis GET returned error: "
                     << (reply->str != nullptr ? reply->str : "unknown");
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (reply->type != REDIS_REPLY_STRING || reply->str == nullptr) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    value = ReplyBytes(reply.get());
    return ErrorCode::OK;
#else
    (void)value;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
}

ErrorCode RedisHaKvBackend::Put(std::string_view key, std::string_view value) {
    auto mapped = MapOpLogKey(key);
    if (!mapped) {
        return mapped.error();
    }
    std::lock_guard<std::mutex> lock(mu_);
    const ErrorCode connected = EnsureConnectedLocked();
    if (connected != ErrorCode::OK) {
        return connected;
    }
#ifdef STORE_USE_REDIS
    auto reply =
        Exec(context_.get(),
             {"EVAL", kPutScript, "2", mapped->index_key, mapped->redis_key,
              std::string(value), std::string(key)});
    if (reply == nullptr) {
        LOG(WARNING) << "Redis PUT failed for OpLog key";
        context_.reset();
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (reply->type == REDIS_REPLY_ERROR ||
        reply->type != REDIS_REPLY_INTEGER || reply->integer != 1) {
        LOG(WARNING) << "Redis PUT script failed";
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
#else
    (void)value;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
}

ErrorCode RedisHaKvBackend::Range(std::string_view begin_key,
                                  std::string_view end_key, size_t limit,
                                  std::vector<KvPair>& kvs) {
    kvs.clear();
    const ErrorCode bounds = ValidateRangeBounds(begin_key, end_key);
    if (bounds != ErrorCode::OK) {
        return bounds;
    }
    if (begin_key == end_key) {
        return ErrorCode::OK;
    }
    auto begin = MapOpLogKey(begin_key);
    auto end = MapOpLogKey(end_key);
    if (!begin || !end) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (begin->cluster_id != end->cluster_id) {
        return ErrorCode::INVALID_PARAMS;
    }
    std::lock_guard<std::mutex> lock(mu_);
    const ErrorCode connected = EnsureConnectedLocked();
    if (connected != ErrorCode::OK) {
        return connected;
    }
#ifdef STORE_USE_REDIS
    auto reply = Exec(
        context_.get(),
        {"EVAL", kRangeScript, "1", begin->index_key,
         std::string("[") + std::string(begin_key),
         std::string("(") + std::string(end_key), std::to_string(limit),
         begin->logical_prefix, begin->redis_prefix});
    if (reply == nullptr) {
        LOG(WARNING) << "Redis RANGE failed for OpLog keys";
        context_.reset();
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        LOG(WARNING) << "Redis RANGE script failed: "
                     << (reply->str != nullptr ? reply->str : "unknown");
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (reply->type != REDIS_REPLY_ARRAY || (reply->elements % 2) != 0) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    kvs.reserve(reply->elements / 2);
    for (size_t i = 0; i < reply->elements; i += 2) {
        const auto* key_reply = reply->element[i];
        const auto* value_reply = reply->element[i + 1];
        if (key_reply == nullptr || value_reply == nullptr ||
            key_reply->type != REDIS_REPLY_STRING ||
            value_reply->type != REDIS_REPLY_STRING) {
            kvs.clear();
            return ErrorCode::ETCD_OPERATION_ERROR;
        }
        kvs.push_back(
            {.key = ReplyBytes(key_reply), .value = ReplyBytes(value_reply)});
    }
    return ErrorCode::OK;
#else
    (void)limit;
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
}

ErrorCode RedisHaKvBackend::DeleteRange(std::string_view begin_key,
                                        std::string_view end_key) {
    const ErrorCode bounds = ValidateRangeBounds(begin_key, end_key);
    if (bounds != ErrorCode::OK) {
        return bounds;
    }
    if (begin_key == end_key) {
        return ErrorCode::OK;
    }
    auto begin = MapOpLogKey(begin_key);
    auto end = MapOpLogKey(end_key);
    if (!begin || !end) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (begin->cluster_id != end->cluster_id) {
        return ErrorCode::INVALID_PARAMS;
    }
    std::lock_guard<std::mutex> lock(mu_);
    const ErrorCode connected = EnsureConnectedLocked();
    if (connected != ErrorCode::OK) {
        return connected;
    }
#ifdef STORE_USE_REDIS
    auto reply = Exec(context_.get(),
                      {"EVAL", kDeleteRangeScript, "1", begin->index_key,
                       std::string("[") + std::string(begin_key),
                       std::string("(") + std::string(end_key),
                       begin->logical_prefix, begin->redis_prefix});
    if (reply == nullptr) {
        LOG(WARNING) << "Redis delete range failed for OpLog keys";
        context_.reset();
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (reply->type == REDIS_REPLY_ERROR ||
        reply->type != REDIS_REPLY_INTEGER || reply->integer != 1) {
        LOG(WARNING) << "Redis delete range script failed";
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    return ErrorCode::OK;
#else
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
}

ErrorCode RedisHaKvBackend::Txn(const KvTxn& txn) {
    for (const auto& compare : txn.compares) {
        if (compare.kind == KvCompareKind::kCreateRevisionEquals) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (compare.kind != KvCompareKind::kValueEquals &&
            compare.kind != KvCompareKind::kKeyNotExists) {
            return ErrorCode::INVALID_PARAMS;
        }
    }
    if (txn.compares.empty() && txn.puts.empty()) {
        return ErrorCode::OK;
    }

    std::vector<std::string> keys;
    std::vector<std::string> args;
    std::string cluster_id;
    std::string index_key;
    auto add_key = [&](std::string_view logical)
        -> tl::expected<int, ErrorCode> {
        auto mapped = MapOpLogKey(logical);
        if (!mapped) {
            return tl::make_unexpected(mapped.error());
        }
        if (cluster_id.empty()) {
            cluster_id = mapped->cluster_id;
            index_key = mapped->index_key;
            keys.push_back(index_key);
        } else if (cluster_id != mapped->cluster_id) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        for (size_t i = 1; i < keys.size(); ++i) {
            if (keys[i] == mapped->redis_key) {
                return static_cast<int>(i + 1);
            }
        }
        keys.push_back(mapped->redis_key);
        return static_cast<int>(keys.size());
    };

    args.push_back(std::to_string(txn.compares.size()));
    args.push_back(std::to_string(txn.puts.size()));
    for (const auto& compare : txn.compares) {
        auto slot = add_key(compare.key);
        if (!slot) {
            return slot.error();
        }
        args.push_back(std::to_string(slot.value()));
        args.push_back(compare.kind == KvCompareKind::kKeyNotExists
                           ? "not_exists"
                           : "value_equals");
        args.push_back(compare.expected_value);
    }
    for (const auto& put : txn.puts) {
        auto slot = add_key(put.key);
        if (!slot) {
            return slot.error();
        }
        args.push_back(std::to_string(slot.value()));
        args.push_back(put.value);
        args.push_back(put.key);
    }

    std::lock_guard<std::mutex> lock(mu_);
    const ErrorCode connected = EnsureConnectedLocked();
    if (connected != ErrorCode::OK) {
        return connected;
    }
#ifdef STORE_USE_REDIS
    std::vector<std::string> command;
    command.reserve(3 + keys.size() + args.size());
    command.emplace_back("EVAL");
    command.emplace_back(kTxnScript);
    command.emplace_back(std::to_string(keys.size()));
    command.insert(command.end(), keys.begin(), keys.end());
    command.insert(command.end(), args.begin(), args.end());
    auto reply = Exec(context_.get(), command);
    if (reply == nullptr) {
        LOG(WARNING) << "Redis OpLog transaction failed at the connection";
        context_.reset();
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        LOG(WARNING) << "Redis OpLog transaction script failed: "
                     << (reply->str != nullptr ? reply->str : "unknown");
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        return ErrorCode::ETCD_OPERATION_ERROR;
    }
    if (reply->integer == 0) {
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    }
    if (reply->integer != 1) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
#else
    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
}

}  // namespace mooncake

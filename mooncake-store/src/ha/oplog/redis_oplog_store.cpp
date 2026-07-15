#ifdef STORE_USE_REDIS

#include "ha/oplog/redis_oplog_store.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <sstream>

#include "ha/oplog/oplog_serializer.h"
#include "ha/oplog/polling_oplog_change_notifier.h"

namespace mooncake {

namespace {

constexpr int kConnectTimeoutMs = 5000;
constexpr int kCommandTimeoutMs = 3000;

bool ParseUint64(const std::string& value, uint64_t& result) {
    try {
        size_t pos = 0;
        result = std::stoull(value, &pos);
        return pos == value.size();
    } catch (...) {
        return false;
    }
}

bool ParseRedisEndpoint(const std::string& redis_endpoint, std::string& host,
                        int& port) {
    host = "127.0.0.1";
    port = 6379;
    auto colon_pos = redis_endpoint.rfind(':');
    if (colon_pos != std::string::npos) {
        host = redis_endpoint.substr(0, colon_pos);
        uint64_t parsed_port = 0;
        const std::string port_str = redis_endpoint.substr(colon_pos + 1);
        if (host.empty() || port_str.empty() ||
            !ParseUint64(port_str, parsed_port) || parsed_port == 0 ||
            parsed_port > 65535) {
            return false;
        }
        port = static_cast<int>(parsed_port);
    } else if (!redis_endpoint.empty()) {
        host = redis_endpoint;
    }
    return !host.empty();
}

std::string Uint64ToString(uint64_t value) { return std::to_string(value); }

std::string SequenceMember(uint64_t value) {
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0') << value;
    return oss.str();
}

}  // namespace

RedisOpLogStore::RedisOpLogStore(const std::string& cluster_id,
                                 const std::string& redis_endpoint,
                                 bool enable_write, int poll_interval_ms,
                                 const std::string& password,
                                 const std::string& username)
    : cluster_id_(cluster_id),
      redis_endpoint_(redis_endpoint),
      username_(username),
      password_(password),
      enable_write_(enable_write),
      poll_interval_ms_(poll_interval_ms) {
    if (!NormalizeAndValidateClusterId(cluster_id_)) {
        LOG(FATAL) << "Invalid cluster_id for RedisOpLogStore: '" << cluster_id
                   << "'. Allowed chars: [A-Za-z0-9_.-], max_len=128.";
    }
    if (cluster_id_.empty()) {
        cluster_id_ = "default";
    }
    key_tag_ = "mooncake:{" + cluster_id_ + "}:oplog";
    index_key_ = key_tag_ + ":index";
    latest_key_ = key_tag_ + ":latest";
    snapshot_prefix_ = key_tag_ + ":snapshot:";
}

RedisOpLogStore::~RedisOpLogStore() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ctx_) {
        redisFree(ctx_);
        ctx_ = nullptr;
    }
}

redisContext* RedisOpLogStore::CreateConnection() const {
    std::string host;
    int port = 0;
    if (!ParseRedisEndpoint(redis_endpoint_, host, port)) {
        LOG(ERROR) << "RedisOpLogStore: invalid Redis endpoint"
                   << ", endpoint=" << redis_endpoint_;
        return nullptr;
    }
    struct timeval connect_timeout;
    connect_timeout.tv_sec = kConnectTimeoutMs / 1000;
    connect_timeout.tv_usec = (kConnectTimeoutMs % 1000) * 1000;

    redisContext* ctx =
        redisConnectWithTimeout(host.c_str(), port, connect_timeout);
    if (!ctx || ctx->err) {
        LOG(ERROR) << "RedisOpLogStore: failed to connect to Redis"
                   << ", endpoint=" << redis_endpoint_
                   << ", error=" << (ctx ? ctx->errstr : "null");
        if (ctx) {
            redisFree(ctx);
        }
        return nullptr;
    }

    struct timeval command_timeout;
    command_timeout.tv_sec = kCommandTimeoutMs / 1000;
    command_timeout.tv_usec = (kCommandTimeoutMs % 1000) * 1000;
    redisSetTimeout(ctx, command_timeout);

    if (!username_.empty()) {
        RedisReplyPtr auth((redisReply*)redisCommand(
            ctx, "AUTH %b %b", username_.data(), username_.size(),
            password_.data(), password_.size()));
        if (!auth || auth->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "RedisOpLogStore: AUTH failed"
                       << ", endpoint=" << redis_endpoint_ << ", error="
                       << (auth && auth->str ? auth->str : "null");
            redisFree(ctx);
            return nullptr;
        }
    } else if (!password_.empty()) {
        RedisReplyPtr auth((redisReply*)redisCommand(
            ctx, "AUTH %b", password_.data(), password_.size()));
        if (!auth || auth->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "RedisOpLogStore: AUTH failed"
                       << ", endpoint=" << redis_endpoint_ << ", error="
                       << (auth && auth->str ? auth->str : "null");
            redisFree(ctx);
            return nullptr;
        }
    }

    return ctx;
}

ErrorCode RedisOpLogStore::EnsureConnectedUnlocked() {
    if (ctx_ && ctx_->err == 0) {
        return ErrorCode::OK;
    }
    if (ctx_) {
        redisFree(ctx_);
        ctx_ = nullptr;
    }
    ctx_ = CreateConnection();
    return ctx_ ? ErrorCode::OK : ErrorCode::INTERNAL_ERROR;
}

ErrorCode RedisOpLogStore::Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        return err;
    }
    if (enable_write_) {
        const std::string zero_seq = SequenceMember(0);
        RedisReplyPtr reply((redisReply*)redisCommand(
            ctx_, "SETNX %b %b", latest_key_.data(), latest_key_.size(),
            zero_seq.data(), zero_seq.size()));
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "RedisOpLogStore::Init: SETNX latest failed";
            return ErrorCode::INTERNAL_ERROR;
        }
    }
    return ErrorCode::OK;
}

std::string RedisOpLogStore::EntryKey(uint64_t sequence_id) const {
    return key_tag_ + ":entry:" + SequenceMember(sequence_id);
}

std::string RedisOpLogStore::SnapshotKey(const std::string& snapshot_id) const {
    return snapshot_prefix_ + snapshot_id;
}

ErrorCode RedisOpLogStore::WriteOpLog(const OpLogEntry& entry, bool /*sync*/) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!enable_write_) {
        LOG(ERROR) << "RedisOpLogStore::WriteOpLog called on reader";
        return ErrorCode::INVALID_PARAMS;
    }
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        return err;
    }

    const std::string entry_key = EntryKey(entry.sequence_id);
    const std::string serialized = SerializeOpLogEntry(entry);
    const std::string seq_member = SequenceMember(entry.sequence_id);
    const std::string zero_seq = SequenceMember(0);
    static constexpr const char* kWriteScript = R"(
local existing = redis.call('GET', KEYS[1])
if existing then
  if existing == ARGV[1] then
    redis.call('ZADD', KEYS[2], 0, ARGV[2])
    local latest = redis.call('GET', KEYS[3]) or ARGV[3]
    if ARGV[2] > latest then redis.call('SET', KEYS[3], ARGV[2]) end
    return 1
  end
  return -1
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], 0, ARGV[2])
local latest = redis.call('GET', KEYS[3]) or ARGV[3]
if ARGV[2] > latest then redis.call('SET', KEYS[3], ARGV[2]) end
return 1
)";

    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx_, "EVAL %s 3 %b %b %b %b %b %b", kWriteScript, entry_key.data(),
        entry_key.size(), index_key_.data(), index_key_.size(),
        latest_key_.data(), latest_key_.size(), serialized.data(),
        serialized.size(), seq_member.data(), seq_member.size(),
        zero_seq.data(), zero_seq.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::WriteOpLog: Redis command failed"
                   << ", sequence_id=" << entry.sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type != REDIS_REPLY_INTEGER || reply->integer != 1) {
        LOG(ERROR) << "RedisOpLogStore::WriteOpLog: fencing violation"
                   << ", sequence_id=" << entry.sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    return ReadOpLogUnlocked(sequence_id, entry);
}

ErrorCode RedisOpLogStore::ReadOpLogUnlocked(uint64_t sequence_id,
                                             OpLogEntry& entry) {
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        return err;
    }

    const std::string entry_key = EntryKey(sequence_id);
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx_, "GET %b", entry_key.data(), entry_key.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLog: GET failed"
                   << ", sequence_id=" << sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLog: unexpected reply type"
                   << ", type=" << reply->type
                   << ", sequence_id=" << sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }

    std::string serialized(reply->str, reply->len);
    if (!DeserializeOpLogEntry(serialized, entry)) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLog: deserialize failed"
                   << ", sequence_id=" << sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::ReadOpLogSince(uint64_t start_sequence_id,
                                          size_t limit,
                                          std::vector<OpLogEntry>& entries) {
    std::lock_guard<std::mutex> lock(mutex_);
    entries.clear();
    if (limit == 0) {
        return ErrorCode::OK;
    }
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        return err;
    }

    const std::string min_member = "(" + SequenceMember(start_sequence_id);
    const std::string limit_str = std::to_string(limit);
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx_, "ZRANGEBYLEX %b %b + LIMIT 0 %b", index_key_.data(),
        index_key_.size(), min_member.data(), min_member.size(),
        limit_str.data(), limit_str.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: ZRANGEBYLEX failed"
                   << ", start_sequence_id=" << start_sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: unexpected reply type"
                   << ", type=" << reply->type;
        return ErrorCode::INTERNAL_ERROR;
    }

    entries.reserve(reply->elements);
    for (size_t i = 0; i < reply->elements; ++i) {
        redisReply* item = reply->element[i];
        if (!item || item->type != REDIS_REPLY_STRING) {
            LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: invalid seq reply";
            return ErrorCode::INTERNAL_ERROR;
        }
        uint64_t sequence_id = 0;
        if (!ParseUint64(std::string(item->str, item->len), sequence_id)) {
            LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: invalid seq value";
            return ErrorCode::INTERNAL_ERROR;
        }
        OpLogEntry entry;
        err = ReadOpLogUnlocked(sequence_id, entry);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: missing indexed "
                          "entry"
                       << ", sequence_id=" << sequence_id
                       << ", error=" << toString(err);
            return err;
        }
        entries.push_back(std::move(entry));
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::GetLatestSequenceId(uint64_t& sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        return err;
    }
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx_, "GET %b", latest_key_.data(), latest_key_.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::GetLatestSequenceId: GET failed";
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        sequence_id = 0;
        return ErrorCode::OK;
    }
    if (reply->type != REDIS_REPLY_STRING ||
        !ParseUint64(std::string(reply->str, reply->len), sequence_id)) {
        LOG(ERROR) << "RedisOpLogStore::GetLatestSequenceId: invalid value";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::GetMaxSequenceId(uint64_t& sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        return err;
    }
    RedisReplyPtr reply(
        (redisReply*)redisCommand(ctx_, "ZREVRANGEBYLEX %b + - LIMIT 0 1",
                                  index_key_.data(), index_key_.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR)
            << "RedisOpLogStore::GetMaxSequenceId: ZREVRANGEBYLEX failed";
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        LOG(ERROR) << "RedisOpLogStore::GetMaxSequenceId: unexpected reply "
                      "type"
                   << ", type=" << reply->type;
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->elements == 0) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }
    redisReply* item = reply->element[0];
    if (!item || item->type != REDIS_REPLY_STRING ||
        !ParseUint64(std::string(item->str, item->len), sequence_id)) {
        LOG(ERROR) << "RedisOpLogStore::GetMaxSequenceId: invalid seq value";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::UpdateLatestSequenceId(uint64_t sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        return err;
    }
    const std::string seq = SequenceMember(sequence_id);
    RedisReplyPtr reply(
        (redisReply*)redisCommand(ctx_, "SET %b %b", latest_key_.data(),
                                  latest_key_.size(), seq.data(), seq.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::UpdateLatestSequenceId: SET failed";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

bool RedisOpLogStore::IsValidSnapshotId(const std::string& snapshot_id) const {
    return !snapshot_id.empty() && snapshot_id.find('/') == std::string::npos &&
           snapshot_id.find("..") == std::string::npos &&
           snapshot_id.find('\0') == std::string::npos;
}

ErrorCode RedisOpLogStore::RecordSnapshotSequenceId(
    const std::string& snapshot_id, uint64_t sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsValidSnapshotId(snapshot_id)) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        return err;
    }
    const std::string key = SnapshotKey(snapshot_id);
    const std::string seq = Uint64ToString(sequence_id);
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx_, "SET %b %b", key.data(), key.size(), seq.data(), seq.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::RecordSnapshotSequenceId: SET failed";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::GetSnapshotSequenceId(const std::string& snapshot_id,
                                                 uint64_t& sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsValidSnapshotId(snapshot_id)) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        return err;
    }
    const std::string key = SnapshotKey(snapshot_id);
    RedisReplyPtr reply(
        (redisReply*)redisCommand(ctx_, "GET %b", key.data(), key.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::GetSnapshotSequenceId: GET failed";
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }
    if (reply->type != REDIS_REPLY_STRING ||
        !ParseUint64(std::string(reply->str, reply->len), sequence_id)) {
        LOG(ERROR) << "RedisOpLogStore::GetSnapshotSequenceId: invalid value";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::CleanupOpLogBefore(uint64_t before_sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (before_sequence_id == 0) {
        return ErrorCode::OK;
    }
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        return err;
    }

    const std::string max_member = "[" + SequenceMember(before_sequence_id - 1);
    static constexpr const char* kCleanupScript = R"(
local members = redis.call('ZRANGEBYLEX', KEYS[1], '-', ARGV[1])
for _, member in ipairs(members) do
  redis.call('DEL', ARGV[2] .. member)
end
redis.call('ZREMRANGEBYLEX', KEYS[1], '-', ARGV[1])
return #members
)";
    const std::string entry_prefix = key_tag_ + ":entry:";
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx_, "EVAL %s 1 %b %b %b", kCleanupScript, index_key_.data(),
        index_key_.size(), max_member.data(), max_member.size(),
        entry_prefix.data(), entry_prefix.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::CleanupOpLogBefore: cleanup failed";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

std::unique_ptr<OpLogChangeNotifier> RedisOpLogStore::CreateChangeNotifier(
    const std::string& /*cluster_id*/) {
    return std::make_unique<PollingOpLogChangeNotifier>(this,
                                                        poll_interval_ms_);
}

}  // namespace mooncake

#endif  // STORE_USE_REDIS

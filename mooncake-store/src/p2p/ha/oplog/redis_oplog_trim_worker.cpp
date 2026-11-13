#ifdef STORE_USE_REDIS

#include "p2p/ha/oplog/redis_oplog_trim_worker.h"

#include <glog/logging.h>
#include <hiredis/hiredis.h>

#include <algorithm>
#include <iomanip>
#include <memory>
#include <sstream>
#include <utility>

#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/ha_status.h"
#include "p2p/ha/oplog/oplog_store.h"
#include "p2p/ha/redis_util.h"

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

std::string SequenceMember(uint64_t value) {
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0') << value;
    return oss.str();
}

std::unique_ptr<redisContext, decltype(&redisFree)> CreateRedisConnection(
    const RedisOpLogTrimWorkerConfig& config) {
    return {
        RedisUtil::CreateConnection(
            config.redis_endpoint, config.redis_username, config.redis_password,
            config.redis_db_index, kConnectTimeoutMs, kCommandTimeoutMs),
        &redisFree};
}

bool IsSnapshotTrimEligibleStandby(const RedisMasterRegistryEntry& entry,
                                   const std::string& self_instance_id) {
    if (!self_instance_id.empty() && entry.instance_id == self_instance_id) {
        return false;
    }
    return entry.role == "standby" && entry.snapshot_ready &&
           !entry.snapshot_endpoint.empty();
}

}  // namespace

uint64_t ComputeRedisOpLogTrimTarget(
    uint64_t latest_sequence_id, uint64_t current_trimmed_sequence_id,
    uint64_t retention_entries, uint32_t min_snapshot_ready_standby_count,
    const std::vector<RedisMasterRegistryEntry>& registry_entries,
    const std::string& self_instance_id) {
    if (retention_entries == 0 || min_snapshot_ready_standby_count == 0 ||
        latest_sequence_id <= retention_entries) {
        return current_trimmed_sequence_id;
    }

    const uint64_t target_by_retention = latest_sequence_id - retention_entries;
    if (target_by_retention <= current_trimmed_sequence_id) {
        return current_trimmed_sequence_id;
    }

    std::vector<uint64_t> snapshot_sequences;
    for (const auto& entry : registry_entries) {
        if (IsSnapshotTrimEligibleStandby(entry, self_instance_id)) {
            snapshot_sequences.push_back(entry.applied_sequence_id);
        }
    }
    if (snapshot_sequences.size() < min_snapshot_ready_standby_count) {
        return current_trimmed_sequence_id;
    }

    std::sort(snapshot_sequences.begin(), snapshot_sequences.end(),
              [](uint64_t lhs, uint64_t rhs) { return lhs > rhs; });
    const uint64_t snapshot_watermark =
        snapshot_sequences[min_snapshot_ready_standby_count - 1];
    return std::max(current_trimmed_sequence_id,
                    std::min(target_by_retention, snapshot_watermark));
}

RedisOpLogTrimWorker::RedisOpLogTrimWorker(RedisOpLogTrimWorkerConfig config)
    : config_(std::move(config)) {
    if (NormalizeAndValidateClusterId(config_.cluster_id) &&
        config_.cluster_id.empty()) {
        config_.cluster_id = "default";
    }
}

RedisOpLogTrimWorker::~RedisOpLogTrimWorker() { Stop(); }

ErrorCode RedisOpLogTrimWorker::ValidateConfig() const {
    std::string cluster_id = config_.cluster_id;
    if (!NormalizeAndValidateClusterId(cluster_id)) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (config_.redis_endpoint.empty() || config_.interval.count() <= 0 ||
        config_.registry_ttl.count() <= 0 || config_.retention_entries == 0 ||
        config_.delete_batch_size == 0 ||
        config_.min_snapshot_ready_standby_count == 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogTrimWorker::Start() {
    auto err = ValidateConfig();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogTrimWorker: invalid configuration"
                   << ", cluster_id=" << config_.cluster_id
                   << ", redis_endpoint=" << config_.redis_endpoint;
        return err;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (thread_.joinable()) {
        return ErrorCode::OK;
    }
    stopping_ = false;
    running_.store(true);
    thread_ = std::thread(&RedisOpLogTrimWorker::Run, this);
    LOG(INFO) << "RedisOpLogTrimWorker started"
              << ", cluster_id=" << config_.cluster_id
              << ", retention_entries=" << config_.retention_entries
              << ", delete_batch_size=" << config_.delete_batch_size
              << ", interval_sec=" << config_.interval.count();
    return ErrorCode::OK;
}

void RedisOpLogTrimWorker::Stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!thread_.joinable()) {
            running_.store(false);
            return;
        }
        stopping_ = true;
    }
    cv_.notify_all();
    thread_.join();
    running_.store(false);
    LOG(INFO) << "RedisOpLogTrimWorker stopped"
              << ", cluster_id=" << config_.cluster_id;
}

std::string RedisOpLogTrimWorker::KeyPrefix() const {
    return "mooncake:" + config_.cluster_id + ":oplog";
}

ErrorCode RedisOpLogTrimWorker::ReadSequenceKey(redisContext* ctx,
                                                const std::string& key,
                                                uint64_t& sequence_id) const {
    RedisReplyPtr reply(
        (redisReply*)redisCommand(ctx, "GET %b", key.data(), key.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogTrimWorker: GET sequence key failed"
                   << ", key=" << key << ", error="
                   << RedisUtil::FormatCommandError(ctx, reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        sequence_id = 0;
        return ErrorCode::OK;
    }
    if (reply->type != REDIS_REPLY_STRING ||
        !ParseUint64(std::string(reply->str, reply->len), sequence_id)) {
        LOG(ERROR) << "RedisOpLogTrimWorker: invalid sequence key value"
                   << ", key=" << key;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogTrimWorker::AdvanceTrimmed(redisContext* ctx,
                                               uint64_t target,
                                               uint64_t& sequence_id) const {
    const std::string trimmed_key = KeyPrefix() + ":trimmed";
    const std::string target_seq = SequenceMember(target);
    static constexpr const char* kAdvanceTrimmedScript =
        "local current = redis.call('GET', KEYS[1]); "
        "if not current then current = '00000000000000000000'; end; "
        "if string.len(current) < 20 then "
        "current = string.rep('0', 20 - string.len(current)) .. current; "
        "end; "
        "if current < ARGV[1] then "
        "redis.call('SET', KEYS[1], ARGV[1]); return ARGV[1]; end; "
        "return current";

    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx, "EVAL %s 1 %b %b", kAdvanceTrimmedScript, trimmed_key.data(),
        trimmed_key.size(), target_seq.data(), target_seq.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogTrimWorker: advance trimmed EVAL failed"
                   << ", target=" << target << ", error="
                   << RedisUtil::FormatCommandError(ctx, reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type != REDIS_REPLY_STRING ||
        !ParseUint64(std::string(reply->str, reply->len), sequence_id)) {
        LOG(ERROR) << "RedisOpLogTrimWorker: invalid advance trimmed reply"
                   << ", reply_type=" << reply->type;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogTrimWorker::CollectGarbage(redisContext* ctx,
                                               uint64_t latest,
                                               uint64_t trimmed,
                                               uint64_t& deleted_to) const {
    deleted_to = 0;
    const std::string key_prefix = KeyPrefix();
    const std::string gc_cursor_key = key_prefix + ":gc_cursor";

    uint64_t gc_cursor = 0;
    auto err = ReadSequenceKey(ctx, gc_cursor_key, gc_cursor);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogTrimWorker: failed to read gc cursor"
                   << ", cluster_id=" << config_.cluster_id
                   << ", latest_sequence_id=" << latest
                   << ", trimmed_sequence_id=" << trimmed
                   << ", error=" << toString(err);
        return err;
    }
    HAMetricManager::instance().set_oplog_gc_cursor_sequence_id(
        static_cast<int64_t>(gc_cursor));
    if (gc_cursor >= trimmed) {
        return ErrorCode::OK;
    }

    const uint64_t batch_count =
        std::min(trimmed - gc_cursor, config_.delete_batch_size);
    const uint64_t batch_start = gc_cursor + 1;
    const uint64_t batch_end = batch_start + batch_count - 1;

    for (uint64_t sequence_id = batch_start; sequence_id <= batch_end;
         ++sequence_id) {
        const std::string entry_key =
            key_prefix + ":entry:" + SequenceMember(sequence_id);
        if (redisAppendCommand(ctx, "DEL %b", entry_key.data(),
                               entry_key.size()) != REDIS_OK) {
            LOG(ERROR) << "RedisOpLogTrimWorker: append DEL failed"
                       << ", sequence_id=" << sequence_id
                       << ", cluster_id=" << config_.cluster_id
                       << ", latest_sequence_id=" << latest
                       << ", trimmed_sequence_id=" << trimmed
                       << ", gc_cursor=" << gc_cursor
                       << ", batch_start=" << batch_start
                       << ", batch_end=" << batch_end
                       << ", batch_count=" << batch_count << ", error="
                       << RedisUtil::FormatCommandError(ctx, nullptr);
            return ErrorCode::INTERNAL_ERROR;
        }
    }
    for (uint64_t sequence_id = batch_start; sequence_id <= batch_end;
         ++sequence_id) {
        redisReply* raw_reply = nullptr;
        if (redisGetReply(ctx, reinterpret_cast<void**>(&raw_reply)) !=
                REDIS_OK ||
            !raw_reply) {
            LOG(ERROR) << "RedisOpLogTrimWorker: DEL failed"
                       << ", sequence_id=" << sequence_id
                       << ", cluster_id=" << config_.cluster_id
                       << ", latest_sequence_id=" << latest
                       << ", trimmed_sequence_id=" << trimmed
                       << ", gc_cursor=" << gc_cursor
                       << ", batch_start=" << batch_start
                       << ", batch_end=" << batch_end
                       << ", batch_count=" << batch_count << ", error="
                       << RedisUtil::FormatCommandError(ctx, raw_reply);
            return ErrorCode::INTERNAL_ERROR;
        }
        RedisReplyPtr del_reply(raw_reply);
        if (del_reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "RedisOpLogTrimWorker: DEL error"
                       << ", sequence_id=" << sequence_id
                       << ", cluster_id=" << config_.cluster_id
                       << ", latest_sequence_id=" << latest
                       << ", trimmed_sequence_id=" << trimmed
                       << ", gc_cursor=" << gc_cursor
                       << ", batch_start=" << batch_start
                       << ", batch_end=" << batch_end
                       << ", batch_count=" << batch_count << ", error="
                       << RedisUtil::FormatCommandError(ctx, del_reply.get());
            return ErrorCode::INTERNAL_ERROR;
        }
    }

    const std::string cursor_seq = SequenceMember(batch_end);
    RedisReplyPtr update_reply((redisReply*)redisCommand(
        ctx, "SET %b %b", gc_cursor_key.data(), gc_cursor_key.size(),
        cursor_seq.data(), cursor_seq.size()));
    if (!update_reply || update_reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogTrimWorker: SET gc cursor failed"
                   << ", gc_cursor=" << batch_end
                   << ", previous_gc_cursor=" << gc_cursor
                   << ", cluster_id=" << config_.cluster_id
                   << ", latest_sequence_id=" << latest
                   << ", trimmed_sequence_id=" << trimmed
                   << ", batch_start=" << batch_start
                   << ", batch_end=" << batch_end
                   << ", batch_count=" << batch_count << ", error="
                   << RedisUtil::FormatCommandError(ctx, update_reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }
    deleted_to = batch_end;
    HAMetricManager::instance().set_oplog_gc_cursor_sequence_id(
        static_cast<int64_t>(deleted_to));
    HAMetricManager::instance().inc_oplog_gc_deleted_entries(
        static_cast<int64_t>(batch_count));
    return ErrorCode::OK;
}

ErrorCode RedisOpLogTrimWorker::TrimOnce() {
    HAStatusManager::instance().SetStandbySnapshotAvailability(0, 0);

    auto ctx = CreateRedisConnection(config_);
    if (!ctx) {
        return ErrorCode::INTERNAL_ERROR;
    }

    const std::string key_prefix = KeyPrefix();
    uint64_t latest_sequence_id = 0;
    auto err =
        ReadSequenceKey(ctx.get(), key_prefix + ":latest", latest_sequence_id);
    if (err != ErrorCode::OK) {
        return err;
    }

    uint64_t trimmed_sequence_id = 0;
    err = ReadSequenceKey(ctx.get(), key_prefix + ":trimmed",
                          trimmed_sequence_id);
    if (err != ErrorCode::OK) {
        return err;
    }
    HAMetricManager::instance().set_oplog_trimmed_sequence_id(
        static_cast<int64_t>(trimmed_sequence_id));

    uint64_t logical_trimmed = trimmed_sequence_id;
    if (latest_sequence_id > config_.retention_entries &&
        latest_sequence_id - config_.retention_entries > trimmed_sequence_id) {
        std::vector<RedisMasterRegistryEntry> entries;
        RedisMasterRegistry registry(
            config_.cluster_id, config_.redis_endpoint, config_.redis_username,
            config_.redis_password, config_.redis_db_index);
        err = registry.DiscoverAlive(config_.registry_ttl, entries);
        if (err != ErrorCode::OK) {
            return err;
        }

        size_t snapshot_ready_count = std::count_if(
            entries.begin(), entries.end(), [&](const auto& entry) {
                return IsSnapshotTrimEligibleStandby(entry,
                                                     config_.self_instance_id);
            });
        HAMetricManager::instance().set_oplog_snapshot_ready_standby_count(
            static_cast<int64_t>(snapshot_ready_count));
        HAStatusManager::instance().SetStandbySnapshotAvailability(
            static_cast<uint32_t>(snapshot_ready_count),
            config_.min_snapshot_ready_standby_count);

        const uint64_t trim_target = ComputeRedisOpLogTrimTarget(
            latest_sequence_id, trimmed_sequence_id, config_.retention_entries,
            config_.min_snapshot_ready_standby_count, entries,
            config_.self_instance_id);
        if (trim_target > trimmed_sequence_id) {
            err = AdvanceTrimmed(ctx.get(), trim_target, logical_trimmed);
            if (err != ErrorCode::OK) {
                return err;
            }
            if (logical_trimmed > trimmed_sequence_id) {
                HAMetricManager::instance().inc_oplog_trim_advance_success();
            }
            HAMetricManager::instance().set_oplog_trimmed_sequence_id(
                static_cast<int64_t>(logical_trimmed));
            LOG(INFO) << "RedisOpLogTrimWorker: advanced trim horizon"
                      << ", cluster_id=" << config_.cluster_id
                      << ", latest_sequence_id=" << latest_sequence_id
                      << ", previous_trimmed_sequence_id="
                      << trimmed_sequence_id
                      << ", trimmed_sequence_id=" << logical_trimmed;
        }
    }

    uint64_t deleted_to = 0;
    err = CollectGarbage(ctx.get(), latest_sequence_id, logical_trimmed,
                         deleted_to);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (deleted_to > 0) {
        VLOG(1) << "RedisOpLogTrimWorker: physical cleanup progressed"
                << ", cluster_id=" << config_.cluster_id
                << ", logical_trimmed=" << logical_trimmed
                << ", deleted_to=" << deleted_to;
    }
    return ErrorCode::OK;
}

void RedisOpLogTrimWorker::Run() {
    while (running_.load()) {
        const auto err = TrimOnce();
        if (err != ErrorCode::OK) {
            HAMetricManager::instance().inc_oplog_trim_failures();
            LOG_EVERY_N(WARNING, 10)
                << "RedisOpLogTrimWorker: trim cycle failed"
                << ", cluster_id=" << config_.cluster_id
                << ", error=" << toString(err)
                << ", interval_sec=" << config_.interval.count();
        }
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, config_.interval, [this] { return stopping_; });
        if (stopping_) {
            break;
        }
    }
}

}  // namespace mooncake

#endif  // STORE_USE_REDIS

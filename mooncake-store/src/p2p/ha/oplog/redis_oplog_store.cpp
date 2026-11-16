#ifdef STORE_USE_REDIS

#include "p2p/ha/oplog/redis_oplog_store.h"

#include <glog/logging.h>
#include <xxhash.h>

#include <chrono>
#include <cstring>
#include <iomanip>
#include <sstream>

#include "p2p/ha/oplog/oplog_serializer.h"
#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/ha/oplog/polling_oplog_change_notifier.h"
#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/ha_status.h"

namespace mooncake {

namespace {

constexpr int kConnectTimeoutMs = 5000;
constexpr int kCommandTimeoutMs = 3000;
constexpr auto kQueueWarningInterval = std::chrono::seconds(5);

bool IsRedisOkStatus(const redisReply* reply) {
    return reply && reply->type == REDIS_REPLY_STATUS && reply->str &&
           reply->len == 2 && std::memcmp(reply->str, "OK", 2) == 0;
}

int64_t CurrentTimestampMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool SetTimeoutUntil(redisContext* ctx,
                     std::chrono::steady_clock::time_point deadline) {
    const auto remaining_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            deadline - std::chrono::steady_clock::now())
            .count();
    if (remaining_us <= 0) return false;
    const struct timeval timeout = {remaining_us / 1000000,
                                    remaining_us % 1000000};
    return redisSetTimeout(ctx, timeout) == REDIS_OK;
}

bool ParseUint64(const std::string& value, uint64_t& result) {
    try {
        size_t pos = 0;
        result = std::stoull(value, &pos);
        return pos == value.size();
    } catch (...) {
        return false;
    }
}

std::string Uint64ToString(uint64_t value) { return std::to_string(value); }

std::string SequenceMember(uint64_t value) {
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0') << value;
    return oss.str();
}

}  // namespace

RedisOpLogStore::RedisOpLogStore(
    const std::string& cluster_id, const std::string& redis_endpoint,
    bool enable_write, int poll_interval_ms, const std::string& password,
    const std::string& username, int db_index, size_t async_queue_max_entries,
    OpLogAsyncQueueOverflowMode overflow_mode, size_t best_effort_max_retries,
    size_t async_write_batch_size)
    : cluster_id_(cluster_id),
      redis_endpoint_(redis_endpoint),
      username_(username),
      password_(password),
      db_index_(db_index),
      enable_write_(enable_write),
      poll_interval_ms_(poll_interval_ms),
      async_queue_max_entries_(async_queue_max_entries),
      async_queue_overflow_mode_(overflow_mode),
      best_effort_max_retries_(best_effort_max_retries),
      async_write_batch_size_(async_write_batch_size) {
    CHECK_GT(async_write_batch_size_, 0);
    CHECK_LE(async_write_batch_size_, kMaxRedisOpLogWriteBatchSize);
    if (!NormalizeAndValidateClusterId(cluster_id_)) {
        LOG(FATAL) << "Invalid cluster_id for RedisOpLogStore: '" << cluster_id
                   << "'. Allowed chars: [A-Za-z0-9_.-/], max_len=128.";
    }
    if (cluster_id_.empty()) {
        cluster_id_ = "default";
    }
    key_prefix_ = "mooncake:" + cluster_id_ + ":oplog";
    latest_key_ = key_prefix_ + ":latest";
    trimmed_key_ = key_prefix_ + ":trimmed";
    snapshot_prefix_ = key_prefix_ + ":snapshot:";
}

RedisOpLogStore::~RedisOpLogStore() {
    StopWriting();
    if (enable_write_) {
        HAStatusManager::instance().SetDoctorIssue("OPLOG_COMMIT_FAILED");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (ctx_) {
        redisFree(ctx_);
        ctx_ = nullptr;
    }
}

redisContext* RedisOpLogStore::CreateConnection() const {
    return RedisUtil::CreateConnection(redis_endpoint_, username_, password_,
                                       db_index_, kConnectTimeoutMs,
                                       kCommandTimeoutMs);
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
    if (!ctx_) {
        LOG(ERROR) << "RedisOpLogStore: failed to create Redis connection"
                   << ", endpoint=" << redis_endpoint_
                   << ", db_index=" << db_index_;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogStore::Init: failed to ensure Redis "
                      "connection"
                   << ", error=" << toString(err);
        return err;
    }
    uint64_t latest = 0;
    if (enable_write_) {
        const std::string zero_seq = SequenceMember(0);
        RedisReplyPtr reply((redisReply*)redisCommand(
            ctx_, "SETNX %b %b", latest_key_.data(), latest_key_.size(),
            zero_seq.data(), zero_seq.size()));
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "RedisOpLogStore::Init: SETNX latest failed"
                       << ", error="
                       << RedisUtil::FormatCommandError(ctx_, reply.get());
            return ErrorCode::INTERNAL_ERROR;
        }
        RedisReplyPtr trim_reply((redisReply*)redisCommand(
            ctx_, "SETNX %b %b", trimmed_key_.data(), trimmed_key_.size(),
            zero_seq.data(), zero_seq.size()));
        if (!trim_reply || trim_reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "RedisOpLogStore::Init: SETNX trimmed failed"
                       << ", error="
                       << RedisUtil::FormatCommandError(ctx_, trim_reply.get());
            return ErrorCode::INTERNAL_ERROR;
        }
    }
    RedisReplyPtr latest_reply((redisReply*)redisCommand(
        ctx_, "GET %b", latest_key_.data(), latest_key_.size()));
    if (!latest_reply || latest_reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::Init: GET latest failed"
                   << ", error="
                   << RedisUtil::FormatCommandError(ctx_, latest_reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }
    if (latest_reply->type == REDIS_REPLY_STRING &&
        !ParseUint64(std::string(latest_reply->str, latest_reply->len),
                     latest)) {
        LOG(ERROR) << "RedisOpLogStore::Init: invalid latest value";
        return ErrorCode::INTERNAL_ERROR;
    }
    HAMetricManager::instance().set_oplog_last_sequence_id(
        static_cast<int64_t>(latest));
    {
        std::lock_guard<std::mutex> async_lock(async_mutex_);
        committed_sequence_id_ = latest;
    }
    if (enable_write_) {
        StartAsyncWorkers();
    }
    return ErrorCode::OK;
}

std::string RedisOpLogStore::EntryKey(uint64_t sequence_id) const {
    return key_prefix_ + ":entry:" + SequenceMember(sequence_id);
}

std::string RedisOpLogStore::SnapshotKey(const std::string& snapshot_id) const {
    return snapshot_prefix_ + snapshot_id;
}

ErrorCode RedisOpLogStore::WriteOpLog(const OpLogEntry& entry, bool sync) {
    return EnqueueWrite(entry, sync);
}

ErrorCode RedisOpLogStore::EnqueueWrite(const OpLogEntry& entry, bool sync) {
    if (!enable_write_) {
        LOG(ERROR) << "RedisOpLogStore::WriteOpLog called on reader";
        return ErrorCode::INVALID_PARAMS;
    }

    std::shared_ptr<PendingWrite> pending;
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        if (!async_running_.load()) {
            VLOG(1) << "RedisOpLogStore: write queue is not running"
                    << ", sequence_id=" << entry.sequence_id
                    << ", sync=" << sync;
            return ErrorCode::INTERNAL_ERROR;
        }

        auto existing = inflight_writes_.find(entry.sequence_id);
        if (existing != inflight_writes_.end()) {
            pending = existing->second;
        } else {
            if (!sync && inflight_writes_.size() >= async_queue_max_entries_) {
                dropped_sequences_.insert(entry.sequence_id);
                async_cv_.notify_one();
                ++queue_overflows_since_last_log_;
                const auto now = std::chrono::steady_clock::now();
                if (next_queue_warning_at_ ==
                        std::chrono::steady_clock::time_point{} ||
                    now >= next_queue_warning_at_) {
                    LOG(WARNING) << "RedisOpLogStore: async queue overflow"
                                 << ", sequence_id=" << entry.sequence_id
                                 << ", queue_size=" << inflight_writes_.size()
                                 << ", mode="
                                 << (async_queue_overflow_mode_ ==
                                             OpLogAsyncQueueOverflowMode::BYPASS
                                         ? "bypass"
                                         : "reject")
                                 << ", occurrences_since_last_log="
                                 << queue_overflows_since_last_log_;
                    queue_overflows_since_last_log_ = 0;
                    next_queue_warning_at_ = now + kQueueWarningInterval;
                }
                if (async_queue_overflow_mode_ ==
                    OpLogAsyncQueueOverflowMode::BYPASS) {
                    HAMetricManager::instance().inc_oplog_queue_bypassed();
                } else {
                    HAMetricManager::instance().inc_oplog_queue_rejected();
                }
                return async_queue_overflow_mode_ ==
                               OpLogAsyncQueueOverflowMode::BYPASS
                           ? ErrorCode::OK
                           : ErrorCode::INTERNAL_ERROR;
            }
            pending = std::make_shared<PendingWrite>(entry, sync);
            inflight_writes_[entry.sequence_id] = pending;
            pending_writes_.push_back(pending);
            UpdateWriteProgressUnlocked();
        }
    }
    async_cv_.notify_one();

    if (!sync) {
        return ErrorCode::OK;
    }

    std::unique_lock<std::mutex> lock(async_mutex_);
    const bool completed = sync_cv_.wait_for(
        lock, async_stop_source_.get_token(),
        std::chrono::milliseconds(kSyncWaitTimeoutMs),
        [&] { return pending->done || !async_running_.load(); });
    if (!completed || !pending->done) {
        if (!async_running_.load()) {
            // StopWriting reports unfinished writes once during cleanup;
            // cancellation is not a timeout and must not flood ERROR logs.
            VLOG(1) << "RedisOpLogStore::WriteOpLog: sync wait cancelled"
                    << ", sequence_id=" << entry.sequence_id;
            return ErrorCode::INTERNAL_ERROR;
        }
        HAMetricManager::instance().inc_oplog_sync_wait_timeouts();
        // TODO(P2P HA): Return an explicit commit-unknown status; this entry
        // remains pending and may still be committed.
        LOG(ERROR) << "RedisOpLogStore::WriteOpLog: sync wait timed out"
                   << ", sequence_id=" << entry.sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }
    return pending->result;
}

void RedisOpLogStore::StartAsyncWorkers() {
    bool expected = false;
    if (!async_running_.compare_exchange_strong(expected, true)) {
        return;
    }
    async_stop_source_ = std::stop_source{};
    HAStatusManager::instance().SetOpLogWriterRunning(true);
    async_workers_.reserve(kAsyncWorkerCount);
    for (size_t i = 0; i < kAsyncWorkerCount; ++i) {
        async_workers_.emplace_back(&RedisOpLogStore::AsyncWriteLoop, this, i);
    }
    HAMetricManager::instance().set_oplog_async_workers_running(true);
}

void RedisOpLogStore::RequestStopWriting() {
    if (!enable_write_) return;
    async_running_.store(false);
    HAStatusManager::instance().SetOpLogWriterRunning(false);
    // Cancellation-aware waits cannot lose a stop between checking their
    // predicate and sleeping. Their notifications do not acquire async_mutex_,
    // which a worker may hold in the future while waiting for Redis I/O.
    async_stop_source_.request_stop();
}

OpLogDrainResult RedisOpLogStore::DrainWriting(uint32_t timeout_ms) {
    OpLogDrainResult result;
    if (!enable_write_) {
        return result;
    }

    std::unique_lock<std::mutex> lock(async_mutex_);
    auto snapshot = [&] {
        result.queued_count = pending_writes_.size();
        result.inflight_count = inflight_writes_.size();
        result.committed_sequence_id = committed_sequence_id_;
        result.max_known_sequence_id = MaxKnownSequenceUnlocked();
        result.drained = pending_writes_.empty() && inflight_writes_.empty();
    };

    snapshot();
    // A lease-loss cancellation can win just before drain starts. Report the
    // actual pending work instead of the default successful result.
    if (result.drained || !async_running_.load() || timeout_ms == 0) {
        return result;
    }
    // A leadership-loss cancellation during an active stepdown drain must not
    // keep the primary waiting for the full timeout after writers are stopped.
    sync_cv_.wait_for(
        lock, async_stop_source_.get_token(),
        std::chrono::milliseconds(timeout_ms), [&] {
            return (pending_writes_.empty() && inflight_writes_.empty()) ||
                   !async_running_.load();
        });
    snapshot();
    return result;
}

void RedisOpLogStore::StopWriting() {
    if (!enable_write_) return;
    RequestStopWriting();
    // Serialize cleanup and joins, including after an earlier nonblocking stop.
    std::lock_guard<std::mutex> stop_lock(async_stop_mutex_);
    {
        std::lock_guard<std::mutex> lock(async_mutex_);
        size_t unfinished_count = 0;
        for (auto& [seq, pending] : inflight_writes_) {
            pending->write_failed = false;
            if (!pending->done) {
                ++unfinished_count;
                pending->done = true;
                pending->result = ErrorCode::INTERNAL_ERROR;
            }
        }
        if (unfinished_count > 0) {
            LOG(ERROR) << "RedisOpLogStore: stopping with unfinished writes"
                       << ", unfinished_count=" << unfinished_count;
        }
        pending_writes_.clear();
        inflight_writes_.clear();
        persisted_sequences_.clear();
        dropped_sequences_.clear();
        failed_write_count_ = 0;
        HAStatusManager::instance().SetDoctorIssue("OPLOG_WRITE_FAILED");
        HAMetricManager::instance().set_oplog_async_queue_size(0);
        sync_cv_.notify_all();
    }
    for (auto& worker : async_workers_) {
        if (worker.joinable()) worker.join();
    }
    async_workers_.clear();
    HAMetricManager::instance().set_oplog_async_workers_running(false);
}

bool RedisOpLogStore::WaitForPendingWrites(
    std::vector<std::shared_ptr<PendingWrite>>& batch) {
    batch.clear();
    std::unique_lock<std::mutex> lock(async_mutex_);
    async_cv_.wait(lock, async_stop_source_.get_token(), [&] {
        return !async_running_.load() || !pending_writes_.empty() ||
               HasCommittableLatestUnlocked();
    });
    if (!async_running_.load()) {
        return false;
    }
    while (!pending_writes_.empty() && batch.size() < async_write_batch_size_) {
        batch.push_back(pending_writes_.front());
        pending_writes_.pop_front();
    }
    return true;
}

void RedisOpLogStore::AsyncWriteLoop(size_t worker_id) {
    redisContext* ctx = CreateConnection();
    std::vector<std::shared_ptr<PendingWrite>> batch;
    batch.reserve(async_write_batch_size_);
    while (WaitForPendingWrites(batch)) {
        if (!ctx || ctx->err) {
            if (ctx) {
                redisFree(ctx);
                ctx = nullptr;
            }
            ctx = CreateConnection();
        }

        if (batch.empty()) {
            bool advanced = false;
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                advanced =
                    ctx && AdvanceCommittedLatestUnlocked(ctx) == ErrorCode::OK;
            }
            if (advanced) {
                continue;
            }
            if (ctx) {
                redisFree(ctx);
                ctx = nullptr;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kAsyncRetryDelayMs));
            continue;
        }

        const auto results = PersistEntriesNoLatest(ctx, batch);
        if (async_running_.load()) {
            HAMetricManager::instance().observe_oplog_redis_pipeline_batch_size(
                static_cast<int64_t>(batch.size()));
            const bool any_failed = std::any_of(
                results.begin(), results.end(), [](const WriteResult& result) {
                    return result.error != ErrorCode::OK;
                });
            if (any_failed) {
                HAMetricManager::instance()
                    .inc_oplog_redis_pipeline_batch_failures();
            }
        }
        std::vector<std::shared_ptr<PendingWrite>> retry_batch;
        for (size_t i = batch.size(); i > 0; --i) {
            const size_t index = i - 1;
            if (FinishOrRetryWrite(ctx, batch[index], results[index].error,
                                   worker_id, results[index].latency_us)) {
                retry_batch.push_back(batch[index]);
            }
        }

        bool latest_failed = false;
        if (async_running_.load()) {
            std::lock_guard<std::mutex> lock(async_mutex_);
            latest_failed =
                ctx && AdvanceCommittedLatestUnlocked(ctx) != ErrorCode::OK;
        }
        if (latest_failed && ctx) {
            redisFree(ctx);
            ctx = nullptr;
        }

        if (!retry_batch.empty()) {
            HAMetricManager::instance()
                .inc_oplog_redis_pipeline_batch_retries();
        }
        if (!retry_batch.empty() || latest_failed) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kAsyncRetryDelayMs));
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                if (async_running_.load()) {
                    for (const auto& pending : retry_batch) {
                        pending_writes_.push_front(pending);
                    }
                }
            }
            async_cv_.notify_one();
        }
    }
    if (ctx) redisFree(ctx);
}

bool RedisOpLogStore::FinishOrRetryWrite(
    redisContext*& ctx, const std::shared_ptr<PendingWrite>& pending,
    ErrorCode err, size_t worker_id, int64_t write_latency_us) {
    if (!async_running_.load()) {
        return false;
    }

    HAMetricManager::instance().observe_oplog_write_latency_us(
        write_latency_us);
    if (err != ErrorCode::OK) {
        HAMetricManager::instance().inc_oplog_write_failures();
        const int64_t consecutive_failures =
            consecutive_write_failures_.fetch_add(1) + 1;
        HAMetricManager::instance().set_oplog_consecutive_write_failures(
            consecutive_failures);
        if (err == ErrorCode::INVALID_PARAMS) {
            CompleteWrite(pending, err);
            return false;
        }
        if (ctx) {
            redisFree(ctx);
            ctx = nullptr;
        }

        size_t attempts = 0;
        {
            std::lock_guard<std::mutex> lock(async_mutex_);
            if (!async_running_.load() || pending->done) {
                return false;
            }
            SetWriteFailedUnlocked(*pending, true);
            attempts = ++pending->attempts;
            if (IsBestEffortP2POpLog(pending->entry.op_type) &&
                attempts >= best_effort_max_retries_) {
                dropped_sequences_.insert(pending->entry.sequence_id);
                SetWriteFailedUnlocked(*pending, false);
                inflight_writes_.erase(pending->entry.sequence_id);
                pending->done = true;
                pending->result = ErrorCode::INTERNAL_ERROR;
                HAMetricManager::instance().inc_oplog_best_effort_dropped();
                UpdateWriteProgressUnlocked();
                sync_cv_.notify_all();
                LOG(ERROR) << "RedisOpLogStore: dropping best-effort oplog"
                           << ", sequence_id=" << pending->entry.sequence_id
                           << ", attempts=" << attempts
                           << ", max_retries=" << best_effort_max_retries_;
                async_cv_.notify_one();
                return false;
            }
        }

        LOG_EVERY_N(WARNING, 100)
            << "RedisOpLogStore: async write failed, retrying"
            << ", worker_id=" << worker_id
            << ", sequence_id=" << pending->entry.sequence_id
            << ", op_type=" << static_cast<int>(pending->entry.op_type)
            << ", attempts=" << attempts
            << ", consecutive_failures=" << consecutive_failures
            << ", error=" << toString(err) << ", sample_every_n=100";
        HAMetricManager::instance().inc_oplog_write_retries();
        return true;
    }

    consecutive_write_failures_.store(0);
    HAMetricManager::instance().set_oplog_consecutive_write_failures(0);
    HAMetricManager::instance().set_oplog_last_successful_write_timestamp_ms(
        CurrentTimestampMs());
    CompleteWrite(pending, ErrorCode::OK);
    return false;
}

std::vector<RedisOpLogStore::WriteResult>
RedisOpLogStore::PersistEntriesNoLatest(
    redisContext* ctx,
    const std::vector<std::shared_ptr<PendingWrite>>& batch) {
    std::vector<WriteResult> results(batch.size());

    if (!ctx || !async_running_.load()) {
        return results;
    }

    const auto started = std::chrono::steady_clock::now();
    const auto deadline =
        started + std::chrono::milliseconds(kCommandTimeoutMs);
    std::vector<std::chrono::steady_clock::time_point> entry_starts(
        batch.size(), started);

    auto fail_batch = [&] {
        const auto now = std::chrono::steady_clock::now();
        for (size_t i = 0; i < results.size(); ++i) {
            results[i].error = ErrorCode::INTERNAL_ERROR;
            results[i].latency_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    now - entry_starts[i])
                    .count();
        }
        return results;
    };

    for (size_t i = 0; i < batch.size(); ++i) {
        if (!async_running_.load()) {
            return fail_batch();
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            LOG_EVERY_N(WARNING, 100)
                << "RedisOpLogStore: pipeline SET batch deadline exceeded "
                   "before append"
                << ", batch_size=" << batch.size() << ", sample_every_n=100";
            return fail_batch();
        }
        entry_starts[i] = std::chrono::steady_clock::now();
        const auto& pending = batch[i];
        const auto entry_key = EntryKey(pending->entry.sequence_id);
        const auto serialized = SerializeOpLogEntry(pending->entry);
        if (redisAppendCommand(ctx, "SET %b %b", entry_key.data(),
                               entry_key.size(), serialized.data(),
                               serialized.size()) != REDIS_OK) {
            LOG_EVERY_N(WARNING, 100)
                << "RedisOpLogStore: pipeline SET append failed"
                << ", first_sequence_id=" << batch.front()->entry.sequence_id
                << ", batch_size=" << batch.size()
                << ", error=" << RedisUtil::FormatCommandError(ctx, nullptr)
                << ", sample_every_n=100";
            return fail_batch();
        }
    }

    for (size_t i = 0; i < batch.size(); ++i) {
        if (!SetTimeoutUntil(ctx, deadline)) {
            LOG_EVERY_N(WARNING, 100)
                << "RedisOpLogStore: pipeline SET batch deadline exceeded"
                << ", sequence_id=" << batch[i]->entry.sequence_id
                << ", batch_size=" << batch.size() << ", sample_every_n=100";
            return fail_batch();
        }

        redisReply* raw_reply = nullptr;
        if (redisGetReply(ctx, reinterpret_cast<void**>(&raw_reply)) !=
            REDIS_OK) {
            LOG_EVERY_N(WARNING, 100)
                << "RedisOpLogStore: pipeline SET reply failed"
                << ", sequence_id=" << batch[i]->entry.sequence_id
                << ", batch_size=" << batch.size()
                << ", error=" << RedisUtil::FormatCommandError(ctx, nullptr)
                << ", sample_every_n=100";
            freeReplyObject(raw_reply);
            return fail_batch();
        }

        RedisReplyPtr reply(raw_reply);
        results[i].latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - entry_starts[i])
                .count();

        if (IsRedisOkStatus(reply.get())) {
            results[i].error = ErrorCode::OK;
        } else {
            LOG_EVERY_N(WARNING, 100)
                << "RedisOpLogStore: pipeline SET command failed"
                << ", sequence_id=" << batch[i]->entry.sequence_id
                << ", error=" << RedisUtil::FormatCommandError(ctx, reply.get())
                << ", sample_every_n=100";
        }
    }

    const struct timeval default_timeout = {kCommandTimeoutMs / 1000,
                                            (kCommandTimeoutMs % 1000) * 1000};
    if (redisSetTimeout(ctx, default_timeout) != REDIS_OK) {
        LOG_EVERY_N(WARNING, 100)
            << "RedisOpLogStore: failed to restore Redis command timeout"
            << ", batch_size=" << batch.size()
            << ", error=" << (ctx->err ? ctx->errstr : "unknown")
            << ", sample_every_n=100";
        return fail_batch();
    }

    return results;
}

void RedisOpLogStore::SetWriteFailedUnlocked(PendingWrite& pending,
                                             bool failed) {
    if (pending.write_failed == failed) return;
    pending.write_failed = failed;
    if (failed) {
        ++failed_write_count_;
    } else {
        DCHECK_GT(failed_write_count_, 0);
        --failed_write_count_;
    }
    // Update the issue under the same queue lock as the per-entry state.
    // Write/retry logs keep concrete sequence IDs and Redis errors.
    HAStatusManager::instance().SetDoctorIssue(
        "OPLOG_WRITE_FAILED",
        failed_write_count_ == 0
            ? ""
            : "unrecovered_writes=" + std::to_string(failed_write_count_));
}

void RedisOpLogStore::UpdateWriteProgressUnlocked() {
    // In-flight includes queued, retrying and persisted-but-uncommitted work.
    // Dropped-only tails intentionally do not keep the stall timer alive.
    // Rewrites at/below committed latest do not require another advance.
    const bool needs_commit =
        !inflight_writes_.empty() &&
        inflight_writes_.rbegin()->first > committed_sequence_id_;
    HAStatusManager::instance().SetOpLogWriteProgress(
        committed_sequence_id_, needs_commit ? inflight_writes_.size() : 0,
        async_queue_max_entries_);
    HAMetricManager::instance().set_oplog_async_queue_size(
        static_cast<int64_t>(inflight_writes_.size()));
}

uint64_t RedisOpLogStore::MaxKnownSequenceUnlocked() const {
    uint64_t max_known_sequence = committed_sequence_id_;
    if (!persisted_sequences_.empty() &&
        *persisted_sequences_.rbegin() > max_known_sequence) {
        max_known_sequence = *persisted_sequences_.rbegin();
    }
    if (!dropped_sequences_.empty() &&
        *dropped_sequences_.rbegin() > max_known_sequence) {
        max_known_sequence = *dropped_sequences_.rbegin();
    }
    return max_known_sequence;
}

bool RedisOpLogStore::HasCommittableLatestUnlocked() const {
    uint64_t scan = committed_sequence_id_;
    const uint64_t max_known_sequence = MaxKnownSequenceUnlocked();
    while (scan < max_known_sequence) {
        const uint64_t next = scan + 1;
        if (persisted_sequences_.count(next) != 0) {
            return true;
        }
        if (dropped_sequences_.count(next) == 0) {
            return false;
        }
        scan = next;
    }
    return false;
}

ErrorCode RedisOpLogStore::AdvanceCommittedLatestUnlocked(redisContext* ctx) {
    if (!async_running_.load()) return ErrorCode::INTERNAL_ERROR;
    uint64_t scan = committed_sequence_id_;
    uint64_t target = committed_sequence_id_;
    const uint64_t max_known_sequence = MaxKnownSequenceUnlocked();
    while (scan < max_known_sequence) {
        const uint64_t next = scan + 1;
        if (persisted_sequences_.count(next) != 0) {
            scan = next;
            target = next;
            continue;
        }
        if (dropped_sequences_.count(next) == 0) {
            break;
        }
        scan = next;
    }
    if (target == committed_sequence_id_) {
        return ErrorCode::OK;
    }

    if (!async_running_.load()) return ErrorCode::INTERNAL_ERROR;
    const std::string seq = SequenceMember(target);
    RedisReplyPtr reply(
        (redisReply*)redisCommand(ctx, "SET %b %b", latest_key_.data(),
                                  latest_key_.size(), seq.data(), seq.size()));
    if (!IsRedisOkStatus(reply.get())) {
        const auto error = RedisUtil::FormatCommandError(ctx, reply.get());
        HAStatusManager::instance().SetDoctorIssue(
            "OPLOG_COMMIT_FAILED",
            "target=" + std::to_string(target) + ": " + error);
        LOG(ERROR) << "RedisOpLogStore::AdvanceCommittedLatest: SET failed"
                   << ", target=" << target << ", error=" << error;
        return ErrorCode::INTERNAL_ERROR;
    }
    HAStatusManager::instance().SetDoctorIssue("OPLOG_COMMIT_FAILED");

    for (uint64_t seq_id = committed_sequence_id_ + 1; seq_id <= target;
         ++seq_id) {
        persisted_sequences_.erase(seq_id);
        dropped_sequences_.erase(seq_id);
        auto it = inflight_writes_.find(seq_id);
        if (it != inflight_writes_.end()) {
            SetWriteFailedUnlocked(*it->second, false);
            it->second->done = true;
            it->second->result = ErrorCode::OK;
            inflight_writes_.erase(it);
        }
    }
    committed_sequence_id_ = target;
    HAMetricManager::instance().set_oplog_last_sequence_id(
        static_cast<int64_t>(target));
    UpdateWriteProgressUnlocked();
    sync_cv_.notify_all();
    return ErrorCode::OK;
}

void RedisOpLogStore::CompleteWrite(
    const std::shared_ptr<PendingWrite>& pending, ErrorCode result) {
    std::lock_guard<std::mutex> lock(async_mutex_);
    if (!async_running_.load() || pending->done) {
        return;
    }
    SetWriteFailedUnlocked(*pending, false);

    if (result != ErrorCode::OK) {
        pending->done = true;
        pending->result = result;
        inflight_writes_.erase(pending->entry.sequence_id);
        UpdateWriteProgressUnlocked();
        sync_cv_.notify_all();
        return;
    }

    if (pending->entry.sequence_id <= committed_sequence_id_) {
        pending->done = true;
        pending->result = ErrorCode::OK;
        inflight_writes_.erase(pending->entry.sequence_id);
        UpdateWriteProgressUnlocked();
        sync_cv_.notify_all();
        return;
    }

    persisted_sequences_.insert(pending->entry.sequence_id);
    UpdateWriteProgressUnlocked();
}

ErrorCode RedisOpLogStore::ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    return ReadOpLogUnlocked(sequence_id, entry);
}

ErrorCode RedisOpLogStore::ReadOpLogUnlocked(uint64_t sequence_id,
                                             OpLogEntry& entry) {
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLog: failed to ensure Redis "
                      "connection"
                   << ", sequence_id=" << sequence_id
                   << ", error=" << toString(err);
        return err;
    }

    const std::string entry_key = EntryKey(sequence_id);
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx_, "GET %b", entry_key.data(), entry_key.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLog: GET failed"
                   << ", sequence_id=" << sequence_id << ", error="
                   << RedisUtil::FormatCommandError(ctx_, reply.get());
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
    OpLogReadProgress progress;
    return ReadOpLogSinceWithProgress(start_sequence_id, limit, entries,
                                      progress);
}

ErrorCode RedisOpLogStore::ReadOpLogSinceWithProgress(
    uint64_t start_sequence_id, size_t limit, std::vector<OpLogEntry>& entries,
    OpLogReadProgress& progress) {
    std::lock_guard<std::mutex> lock(mutex_);
    entries.clear();
    progress.last_scanned_sequence_id = start_sequence_id;
    if (limit == 0) {
        return ErrorCode::OK;
    }
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: failed to ensure "
                      "Redis connection"
                   << ", start_sequence_id=" << start_sequence_id
                   << ", limit=" << limit << ", error=" << toString(err);
        return err;
    }

    uint64_t latest_sequence_id = 0;
    RedisReplyPtr latest_reply((redisReply*)redisCommand(
        ctx_, "GET %b", latest_key_.data(), latest_key_.size()));
    if (!latest_reply || latest_reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: GET latest failed"
                   << ", start_sequence_id=" << start_sequence_id << ", error="
                   << RedisUtil::FormatCommandError(ctx_, latest_reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }
    if (latest_reply->type == REDIS_REPLY_STRING &&
        !ParseUint64(std::string(latest_reply->str, latest_reply->len),
                     latest_sequence_id)) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: invalid latest value";
        return ErrorCode::INTERNAL_ERROR;
    }
    HAMetricManager::instance().set_oplog_last_sequence_id(
        static_cast<int64_t>(latest_sequence_id));

    uint64_t trimmed_sequence_id = 0;
    RedisReplyPtr trimmed_reply((redisReply*)redisCommand(
        ctx_, "GET %b", trimmed_key_.data(), trimmed_key_.size()));
    if (!trimmed_reply || trimmed_reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: GET trimmed failed"
                   << ", start_sequence_id=" << start_sequence_id << ", error="
                   << RedisUtil::FormatCommandError(ctx_, trimmed_reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }
    if (trimmed_reply->type == REDIS_REPLY_STRING &&
        !ParseUint64(std::string(trimmed_reply->str, trimmed_reply->len),
                     trimmed_sequence_id)) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: invalid trimmed value";
        return ErrorCode::INTERNAL_ERROR;
    }

    if (start_sequence_id < trimmed_sequence_id) {
        LOG(WARNING) << "RedisOpLogStore::ReadOpLogSince: requested range was "
                        "trimmed"
                     << ", start_sequence_id=" << start_sequence_id
                     << ", trimmed_sequence_id=" << trimmed_sequence_id;
        return ErrorCode::OPLOG_TRIMMED;
    }
    const uint64_t effective_start = start_sequence_id;
    progress.last_scanned_sequence_id = effective_start;
    if (effective_start >= latest_sequence_id) {
        return ErrorCode::OK;
    }

    entries.reserve(limit);
    uint64_t next_sequence_id = effective_start + 1;
    size_t scanned_count = 0;
    size_t missing_count = 0;
    while (next_sequence_id <= latest_sequence_id && scanned_count < limit) {
        const uint64_t remaining = latest_sequence_id - next_sequence_id + 1;
        const size_t batch_size = static_cast<size_t>(std::min<uint64_t>(
            remaining, static_cast<uint64_t>(limit - scanned_count)));
        const uint64_t batch_start = next_sequence_id;

        for (size_t i = 0; i < batch_size; ++i) {
            const uint64_t sequence_id = batch_start + i;
            const std::string entry_key = EntryKey(sequence_id);
            if (redisAppendCommand(ctx_, "GET %b", entry_key.data(),
                                   entry_key.size()) != REDIS_OK) {
                LOG(ERROR)
                    << "RedisOpLogStore::ReadOpLogSince: append GET failed"
                    << ", sequence_id=" << sequence_id << ", error="
                    << RedisUtil::FormatCommandError(ctx_, nullptr);
                redisFree(ctx_);
                ctx_ = nullptr;
                entries.clear();
                return ErrorCode::INTERNAL_ERROR;
            }
        }

        for (size_t i = 0; i < batch_size; ++i) {
            const uint64_t sequence_id = batch_start + i;
            redisReply* raw_reply = nullptr;
            if (redisGetReply(ctx_, reinterpret_cast<void**>(&raw_reply)) !=
                    REDIS_OK ||
                !raw_reply) {
                LOG(ERROR)
                    << "RedisOpLogStore::ReadOpLogSince: pipeline GET failed"
                    << ", sequence_id=" << sequence_id << ", error="
                    << RedisUtil::FormatCommandError(ctx_, raw_reply);
                redisFree(ctx_);
                ctx_ = nullptr;
                entries.clear();
                return ErrorCode::INTERNAL_ERROR;
            }
            RedisReplyPtr entry_reply(raw_reply);
            if (entry_reply->type == REDIS_REPLY_NIL) {
                // The committed range has no Redis entry at this ID. Deliver
                // a transient NOOP so the applier can advance in order.
                OpLogEntry noop;
                noop.sequence_id = sequence_id;
                noop.op_type = OpType_NOOP;
                noop.checksum = static_cast<uint32_t>(XXH32("", 0, 0));
                entries.push_back(std::move(noop));
                ++missing_count;
                continue;
            }
            if (entry_reply->type != REDIS_REPLY_STRING) {
                LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: unexpected GET "
                              "reply type"
                           << ", type=" << entry_reply->type
                           << ", sequence_id=" << sequence_id;
                redisFree(ctx_);
                ctx_ = nullptr;
                entries.clear();
                return ErrorCode::INTERNAL_ERROR;
            }

            if (entries.size() < limit) {
                OpLogEntry entry;
                std::string serialized(entry_reply->str, entry_reply->len);
                if (!DeserializeOpLogEntry(serialized, entry)) {
                    LOG(ERROR)
                        << "RedisOpLogStore::ReadOpLogSince: deserialize failed"
                        << ", sequence_id=" << sequence_id;
                    redisFree(ctx_);
                    ctx_ = nullptr;
                    entries.clear();
                    return ErrorCode::INTERNAL_ERROR;
                }
                entries.push_back(std::move(entry));
            }
        }
        next_sequence_id += batch_size;
        scanned_count += batch_size;
        progress.last_scanned_sequence_id = next_sequence_id - 1;
    }
    if (missing_count > 0) {
        HAMetricManager::instance().inc_oplog_confirmed_holes(
            static_cast<int64_t>(missing_count));
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::GetLatestSequenceId(uint64_t& sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogStore::GetLatestSequenceId: failed to ensure "
                      "Redis connection"
                   << ", error=" << toString(err);
        return err;
    }
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx_, "GET %b", latest_key_.data(), latest_key_.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::GetLatestSequenceId: GET failed"
                   << ", error="
                   << RedisUtil::FormatCommandError(ctx_, reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        sequence_id = 0;
        HAMetricManager::instance().set_oplog_last_sequence_id(0);
        return ErrorCode::OK;
    }
    if (reply->type != REDIS_REPLY_STRING ||
        !ParseUint64(std::string(reply->str, reply->len), sequence_id)) {
        LOG(ERROR) << "RedisOpLogStore::GetLatestSequenceId: invalid value";
        return ErrorCode::INTERNAL_ERROR;
    }
    HAMetricManager::instance().set_oplog_last_sequence_id(
        static_cast<int64_t>(sequence_id));
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::GetLocalCommittedSequenceId(uint64_t& sequence_id) {
    std::lock_guard<std::mutex> lock(async_mutex_);
    sequence_id = committed_sequence_id_;
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::GetMaxSequenceId(uint64_t& sequence_id) {
    auto err = GetLatestSequenceId(sequence_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogStore::GetMaxSequenceId: failed to get latest "
                      "sequence ID"
                   << ", error=" << toString(err);
        return err;
    }
    if (sequence_id == 0) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::GetTrimmedSequenceId(uint64_t& sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogStore::GetTrimmedSequenceId: failed to "
                      "ensure Redis connection"
                   << ", error=" << toString(err);
        return err;
    }
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx_, "GET %b", trimmed_key_.data(), trimmed_key_.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::GetTrimmedSequenceId: GET failed"
                   << ", error="
                   << RedisUtil::FormatCommandError(ctx_, reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        sequence_id = 0;
        return ErrorCode::OK;
    }
    if (reply->type != REDIS_REPLY_STRING ||
        !ParseUint64(std::string(reply->str, reply->len), sequence_id)) {
        LOG(ERROR) << "RedisOpLogStore::GetTrimmedSequenceId: invalid value";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

// Recovery/admin operation inherited from OpLogStore. It must not run
// concurrently with normal writes.
ErrorCode RedisOpLogStore::SetLocalCommittedSequenceId(uint64_t sequence_id) {
    if (!enable_write_) {
        LOG(ERROR) << "RedisOpLogStore::SetLocalCommittedSequenceId requires "
                      "a writer store"
                   << ", sequence_id=" << sequence_id;
        return ErrorCode::INVALID_PARAMS;
    }

    std::lock_guard<std::mutex> lock(async_mutex_);
    if (sequence_id < committed_sequence_id_ || !pending_writes_.empty() ||
        !inflight_writes_.empty()) {
        LOG(ERROR) << "RedisOpLogStore::SetLocalCommittedSequenceId rejected"
                   << ", sequence_id=" << sequence_id
                   << ", committed_sequence_id=" << committed_sequence_id_
                   << ", pending_size=" << pending_writes_.size()
                   << ", inflight_size=" << inflight_writes_.size();
        return ErrorCode::INVALID_PARAMS;
    }

    committed_sequence_id_ = sequence_id;
    LOG(INFO) << "RedisOpLogStore local committed sequence initialized"
              << ", sequence_id=" << sequence_id;
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::UpdateLatestSequenceId(uint64_t sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogStore::UpdateLatestSequenceId: failed to "
                      "ensure Redis connection"
                   << ", sequence_id=" << sequence_id
                   << ", error=" << toString(err);
        return err;
    }
    const std::string seq = SequenceMember(sequence_id);
    RedisReplyPtr reply(
        (redisReply*)redisCommand(ctx_, "SET %b %b", latest_key_.data(),
                                  latest_key_.size(), seq.data(), seq.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::UpdateLatestSequenceId: SET failed"
                   << ", sequence_id=" << sequence_id << ", error="
                   << RedisUtil::FormatCommandError(ctx_, reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }
    {
        std::lock_guard<std::mutex> async_lock(async_mutex_);
        committed_sequence_id_ = sequence_id;
        persisted_sequences_.erase(
            persisted_sequences_.begin(),
            persisted_sequences_.upper_bound(sequence_id));
        dropped_sequences_.erase(dropped_sequences_.begin(),
                                 dropped_sequences_.upper_bound(sequence_id));
        if (enable_write_) UpdateWriteProgressUnlocked();
    }
    sync_cv_.notify_all();
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
        LOG(ERROR) << "RedisOpLogStore::RecordSnapshotSequenceId: invalid "
                      "snapshot_id"
                   << ", snapshot_id=" << snapshot_id;
        return ErrorCode::INVALID_PARAMS;
    }
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogStore::RecordSnapshotSequenceId: failed to "
                      "ensure Redis connection"
                   << ", snapshot_id=" << snapshot_id
                   << ", sequence_id=" << sequence_id
                   << ", error=" << toString(err);
        return err;
    }
    const std::string key = SnapshotKey(snapshot_id);
    const std::string seq = Uint64ToString(sequence_id);
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx_, "SET %b %b", key.data(), key.size(), seq.data(), seq.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::RecordSnapshotSequenceId: SET failed"
                   << ", snapshot_id=" << snapshot_id
                   << ", sequence_id=" << sequence_id << ", error="
                   << RedisUtil::FormatCommandError(ctx_, reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::GetSnapshotSequenceId(const std::string& snapshot_id,
                                                 uint64_t& sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsValidSnapshotId(snapshot_id)) {
        LOG(ERROR) << "RedisOpLogStore::GetSnapshotSequenceId: invalid "
                      "snapshot_id"
                   << ", snapshot_id=" << snapshot_id;
        return ErrorCode::INVALID_PARAMS;
    }
    auto err = EnsureConnectedUnlocked();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RedisOpLogStore::GetSnapshotSequenceId: failed to "
                      "ensure Redis connection"
                   << ", snapshot_id=" << snapshot_id
                   << ", error=" << toString(err);
        return err;
    }
    const std::string key = SnapshotKey(snapshot_id);
    RedisReplyPtr reply(
        (redisReply*)redisCommand(ctx_, "GET %b", key.data(), key.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::GetSnapshotSequenceId: GET failed"
                   << ", snapshot_id=" << snapshot_id << ", error="
                   << RedisUtil::FormatCommandError(ctx_, reply.get());
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
    if (before_sequence_id == 0) {
        return ErrorCode::OK;
    }
    const uint64_t requested_target = before_sequence_id - 1;

    while (true) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto err = EnsureConnectedUnlocked();
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "RedisOpLogStore::CleanupOpLogBefore: failed to "
                          "ensure Redis connection"
                       << ", before_sequence_id=" << before_sequence_id
                       << ", error=" << toString(err);
            return err;
        }

        uint64_t latest_sequence_id = 0;
        RedisReplyPtr latest_reply((redisReply*)redisCommand(
            ctx_, "GET %b", latest_key_.data(), latest_key_.size()));
        if (!latest_reply || latest_reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR)
                << "RedisOpLogStore::CleanupOpLogBefore: GET latest failed"
                << ", before_sequence_id=" << before_sequence_id << ", error="
                << RedisUtil::FormatCommandError(ctx_, latest_reply.get());
            return ErrorCode::INTERNAL_ERROR;
        }
        if (latest_reply->type == REDIS_REPLY_STRING &&
            !ParseUint64(std::string(latest_reply->str, latest_reply->len),
                         latest_sequence_id)) {
            LOG(ERROR)
                << "RedisOpLogStore::CleanupOpLogBefore: invalid latest value";
            return ErrorCode::INTERNAL_ERROR;
        }

        uint64_t trimmed_sequence_id = 0;
        RedisReplyPtr trimmed_reply((redisReply*)redisCommand(
            ctx_, "GET %b", trimmed_key_.data(), trimmed_key_.size()));
        if (!trimmed_reply || trimmed_reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR)
                << "RedisOpLogStore::CleanupOpLogBefore: GET trimmed failed"
                << ", before_sequence_id=" << before_sequence_id << ", error="
                << RedisUtil::FormatCommandError(ctx_, trimmed_reply.get());
            return ErrorCode::INTERNAL_ERROR;
        }
        if (trimmed_reply->type == REDIS_REPLY_STRING &&
            !ParseUint64(std::string(trimmed_reply->str, trimmed_reply->len),
                         trimmed_sequence_id)) {
            LOG(ERROR)
                << "RedisOpLogStore::CleanupOpLogBefore: invalid trimmed value";
            return ErrorCode::INTERNAL_ERROR;
        }

        const uint64_t cleanup_target =
            std::min(requested_target, latest_sequence_id);
        if (cleanup_target <= trimmed_sequence_id) {
            return ErrorCode::OK;
        }

        const uint64_t batch_start = trimmed_sequence_id + 1;
        const uint64_t remaining = cleanup_target - trimmed_sequence_id;
        const uint64_t batch_count =
            std::min<uint64_t>(remaining, kCleanupBatchSize);
        const uint64_t batch_end = batch_start + batch_count - 1;

        for (uint64_t sequence_id = batch_start; sequence_id <= batch_end;
             ++sequence_id) {
            const std::string entry_key = EntryKey(sequence_id);
            if (redisAppendCommand(ctx_, "DEL %b", entry_key.data(),
                                   entry_key.size()) != REDIS_OK) {
                LOG(ERROR)
                    << "RedisOpLogStore::CleanupOpLogBefore: append DEL failed"
                    << ", sequence_id=" << sequence_id << ", error="
                    << RedisUtil::FormatCommandError(ctx_, nullptr);
                redisFree(ctx_);
                ctx_ = nullptr;
                return ErrorCode::INTERNAL_ERROR;
            }
        }

        for (uint64_t sequence_id = batch_start; sequence_id <= batch_end;
             ++sequence_id) {
            redisReply* raw_reply = nullptr;
            if (redisGetReply(ctx_, reinterpret_cast<void**>(&raw_reply)) !=
                    REDIS_OK ||
                !raw_reply) {
                LOG(ERROR) << "RedisOpLogStore::CleanupOpLogBefore: DEL failed"
                           << ", sequence_id=" << sequence_id << ", error="
                           << RedisUtil::FormatCommandError(ctx_, raw_reply);
                redisFree(ctx_);
                ctx_ = nullptr;
                return ErrorCode::INTERNAL_ERROR;
            }
            RedisReplyPtr del_reply(raw_reply);
            if (del_reply->type == REDIS_REPLY_ERROR) {
                LOG(ERROR) << "RedisOpLogStore::CleanupOpLogBefore: DEL error"
                           << ", sequence_id=" << sequence_id << ", error="
                           << RedisUtil::FormatCommandError(ctx_,
                                                            del_reply.get());
                redisFree(ctx_);
                ctx_ = nullptr;
                return ErrorCode::INTERNAL_ERROR;
            }
        }

        const std::string trimmed_seq = SequenceMember(batch_end);
        RedisReplyPtr update_reply((redisReply*)redisCommand(
            ctx_, "SET %b %b", trimmed_key_.data(), trimmed_key_.size(),
            trimmed_seq.data(), trimmed_seq.size()));
        if (!update_reply || update_reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR)
                << "RedisOpLogStore::CleanupOpLogBefore: SET trimmed failed"
                << ", trimmed_sequence_id=" << batch_end << ", error="
                << RedisUtil::FormatCommandError(ctx_, update_reply.get());
            return ErrorCode::INTERNAL_ERROR;
        }
    }
}

std::unique_ptr<OpLogChangeNotifier> RedisOpLogStore::CreateChangeNotifier(
    const std::string& /*cluster_id*/, size_t poll_batch_size) {
    return std::make_unique<PollingOpLogChangeNotifier>(this, poll_interval_ms_,
                                                        poll_batch_size);
}

}  // namespace mooncake

#endif  // STORE_USE_REDIS

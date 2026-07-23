#ifdef STORE_USE_REDIS

#include "ha/oplog/redis_oplog_store.h"

#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <iomanip>
#include <sstream>

#include "ha/oplog/oplog_serializer.h"
#include "ha/oplog/p2p_oplog_types.h"
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
    OpLogAsyncQueueOverflowMode overflow_mode, size_t best_effort_max_retries)
    : cluster_id_(cluster_id),
      redis_endpoint_(redis_endpoint),
      username_(username),
      password_(password),
      db_index_(db_index),
      enable_write_(enable_write),
      poll_interval_ms_(poll_interval_ms),
      async_queue_max_entries_(async_queue_max_entries),
      async_queue_overflow_mode_(overflow_mode),
      best_effort_max_retries_(best_effort_max_retries) {
    if (!NormalizeAndValidateClusterId(cluster_id_)) {
        LOG(FATAL) << "Invalid cluster_id for RedisOpLogStore: '" << cluster_id
                   << "'. Allowed chars: [A-Za-z0-9_.-], max_len=128.";
    }
    if (cluster_id_.empty()) {
        cluster_id_ = "default";
    }
    key_tag_ = "mooncake:{" + cluster_id_ + "}:oplog";
    latest_key_ = key_tag_ + ":latest";
    trimmed_key_ = key_tag_ + ":trimmed";
    snapshot_prefix_ = key_tag_ + ":snapshot:";
}

RedisOpLogStore::~RedisOpLogStore() {
    StopAsyncWorkers();
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
            LOG(ERROR) << "RedisOpLogStore::Init: SETNX latest failed";
            return ErrorCode::INTERNAL_ERROR;
        }
        RedisReplyPtr trim_reply((redisReply*)redisCommand(
            ctx_, "SETNX %b %b", trimmed_key_.data(), trimmed_key_.size(),
            zero_seq.data(), zero_seq.size()));
        if (!trim_reply || trim_reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "RedisOpLogStore::Init: SETNX trimmed failed";
            return ErrorCode::INTERNAL_ERROR;
        }
    }
    RedisReplyPtr latest_reply((redisReply*)redisCommand(
        ctx_, "GET %b", latest_key_.data(), latest_key_.size()));
    if (!latest_reply || latest_reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::Init: GET latest failed";
        return ErrorCode::INTERNAL_ERROR;
    }
    if (latest_reply->type == REDIS_REPLY_STRING &&
        !ParseUint64(std::string(latest_reply->str, latest_reply->len),
                     latest)) {
        LOG(ERROR) << "RedisOpLogStore::Init: invalid latest value";
        return ErrorCode::INTERNAL_ERROR;
    }
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
    return key_tag_ + ":entry:" + SequenceMember(sequence_id);
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
            LOG(ERROR) << "RedisOpLogStore: write queue is not running"
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
                LOG(WARNING)
                    << "RedisOpLogStore: async queue overflow"
                    << ", sequence_id=" << entry.sequence_id
                    << ", queue_size=" << inflight_writes_.size() << ", mode="
                    << (async_queue_overflow_mode_ ==
                                OpLogAsyncQueueOverflowMode::BYPASS
                            ? "bypass"
                            : "reject");
                return async_queue_overflow_mode_ ==
                               OpLogAsyncQueueOverflowMode::BYPASS
                           ? ErrorCode::OK
                           : ErrorCode::INTERNAL_ERROR;
            }
            pending = std::make_shared<PendingWrite>(entry, sync);
            inflight_writes_[entry.sequence_id] = pending;
            pending_writes_.push_back(pending);
        }
    }
    async_cv_.notify_one();

    if (!sync) {
        return ErrorCode::OK;
    }

    std::unique_lock<std::mutex> lock(async_mutex_);
    bool completed = sync_cv_.wait_for(
        lock, std::chrono::milliseconds(kSyncWaitTimeoutMs),
        [&] { return pending->done || !async_running_.load(); });
    if (!completed) {
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
    async_workers_.reserve(kAsyncWorkerCount);
    for (size_t i = 0; i < kAsyncWorkerCount; ++i) {
        async_workers_.emplace_back(&RedisOpLogStore::AsyncWriteLoop, this, i);
    }
}

void RedisOpLogStore::StopAsyncWorkers() {
    // TODO(P2P HA): Support graceful flush/drain before stopping workers.
    if (!async_running_.exchange(false)) {
        return;
    }
    async_cv_.notify_all();
    for (auto& worker : async_workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    async_workers_.clear();

    std::lock_guard<std::mutex> lock(async_mutex_);
    size_t unfinished_count = 0;
    for (const auto& [seq, pending] : inflight_writes_) {
        if (!pending->done) {
            ++unfinished_count;
        }
    }
    if (unfinished_count > 0) {
        LOG(ERROR) << "RedisOpLogStore: stopping with unfinished writes"
                   << ", unfinished_count=" << unfinished_count;
    }
    for (auto& [seq, pending] : inflight_writes_) {
        if (!pending->done) {
            pending->done = true;
            pending->result = ErrorCode::INTERNAL_ERROR;
        }
    }
    sync_cv_.notify_all();
}

void RedisOpLogStore::AsyncWriteLoop(size_t worker_id) {
    redisContext* ctx = CreateConnection();
    while (async_running_.load()) {
        std::shared_ptr<PendingWrite> pending;
        {
            std::unique_lock<std::mutex> lock(async_mutex_);
            async_cv_.wait(lock, [&] {
                return !async_running_.load() || !pending_writes_.empty() ||
                       dropped_sequences_.count(committed_sequence_id_ + 1) !=
                           0;
            });
            if (!async_running_.load() && pending_writes_.empty()) {
                break;
            }
            if (!pending_writes_.empty()) {
                pending = pending_writes_.front();
                pending_writes_.pop_front();
            }
        }

        if (!ctx || ctx->err) {
            if (ctx) {
                redisFree(ctx);
            }
            ctx = CreateConnection();
        }
        // TODO(P2P HA): Batch several queued entries per worker and pipeline
        // their SET commands to reduce Redis round trips. Keep the current
        // one-entry retry path until batch error handling and partial reply
        // draining are covered by tests.
        if (!pending) {
            bool advanced = false;
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                advanced =
                    ctx && AdvanceCommittedLatestUnlocked(ctx) == ErrorCode::OK;
            }
            if (advanced) continue;
            if (ctx) {
                redisFree(ctx);
                ctx = nullptr;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kAsyncRetryDelayMs));
            continue;
        }
        ErrorCode err = ctx ? PersistEntryNoLatest(ctx, pending->entry)
                            : ErrorCode::INTERNAL_ERROR;
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_PARAMS) {
                CompleteWrite(ctx, pending, err);
                continue;
            }
            if (ctx) {
                redisFree(ctx);
                ctx = nullptr;
            }
            size_t attempts;
            {
                // Mutable PendingWrite state is protected by async_mutex_.
                std::lock_guard<std::mutex> lock(async_mutex_);
                attempts = ++pending->attempts;
                if (IsBestEffortRedisOpLog(pending->entry.op_type) &&
                    attempts >= best_effort_max_retries_) {
                    dropped_sequences_.insert(pending->entry.sequence_id);
                    inflight_writes_.erase(pending->entry.sequence_id);
                    pending->done = true;
                    pending->result = ErrorCode::INTERNAL_ERROR;
                    sync_cv_.notify_all();
                    LOG(ERROR) << "RedisOpLogStore: dropping best-effort oplog"
                               << ", sequence_id=" << pending->entry.sequence_id
                               << ", attempts=" << attempts;
                    async_cv_.notify_one();
                    continue;
                }
            }
            // TODO(P2P HA): Add bounded exponential backoff, jitter, and
            // pending/retry metrics.
            LOG(WARNING) << "RedisOpLogStore: async write failed, retrying"
                         << ", worker_id=" << worker_id
                         << ", sequence_id=" << pending->entry.sequence_id
                         << ", attempts=" << attempts
                         << ", error=" << toString(err);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kAsyncRetryDelayMs));
            {
                std::lock_guard<std::mutex> lock(async_mutex_);
                if (async_running_.load()) {
                    pending_writes_.push_front(pending);
                }
            }
            async_cv_.notify_one();
            continue;
        }

        CompleteWrite(ctx, pending, ErrorCode::OK);
    }
    if (ctx) {
        redisFree(ctx);
    }
}

ErrorCode RedisOpLogStore::PersistEntryNoLatest(redisContext* ctx,
                                                const OpLogEntry& entry) {
    const std::string entry_key = EntryKey(entry.sequence_id);
    const std::string serialized = SerializeOpLogEntry(entry);
    // A previous Primary may leave an uncommitted entry beyond latest; the
    // current Primary must be able to overwrite it and continue the sequence.
    // TODO(P2P HA): Atomically reject conflicting overwrites at or below the
    // committed latest while still allowing overwrites beyond latest.
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx, "SET %b %b", entry_key.data(), entry_key.size(), serialized.data(),
        serialized.size()));
    if (!reply || reply->type != REDIS_REPLY_STATUS) {
        LOG(ERROR) << "RedisOpLogStore::PersistEntryNoLatest: Redis command "
                      "failed"
                   << ", sequence_id=" << entry.sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode RedisOpLogStore::AdvanceCommittedLatestUnlocked(redisContext* ctx) {
    uint64_t target = committed_sequence_id_;
    while (persisted_sequences_.count(target + 1) != 0 ||
           dropped_sequences_.count(target + 1) != 0) {
        ++target;
    }
    if (target == committed_sequence_id_) {
        return ErrorCode::OK;
    }

    const std::string seq = SequenceMember(target);
    RedisReplyPtr reply(
        (redisReply*)redisCommand(ctx, "SET %b %b", latest_key_.data(),
                                  latest_key_.size(), seq.data(), seq.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::AdvanceCommittedLatest: SET failed"
                   << ", target=" << target;
        return ErrorCode::INTERNAL_ERROR;
    }

    for (uint64_t seq_id = committed_sequence_id_ + 1; seq_id <= target;
         ++seq_id) {
        persisted_sequences_.erase(seq_id);
        dropped_sequences_.erase(seq_id);
        auto it = inflight_writes_.find(seq_id);
        if (it != inflight_writes_.end()) {
            it->second->done = true;
            it->second->result = ErrorCode::OK;
            inflight_writes_.erase(it);
        }
    }
    committed_sequence_id_ = target;
    sync_cv_.notify_all();
    return ErrorCode::OK;
}

void RedisOpLogStore::CompleteWrite(
    redisContext* ctx, const std::shared_ptr<PendingWrite>& pending,
    ErrorCode result) {
    std::lock_guard<std::mutex> lock(async_mutex_);
    if (result != ErrorCode::OK) {
        pending->done = true;
        pending->result = result;
        inflight_writes_.erase(pending->entry.sequence_id);
        sync_cv_.notify_all();
        return;
    }

    if (pending->entry.sequence_id <= committed_sequence_id_) {
        pending->done = true;
        pending->result = ErrorCode::OK;
        inflight_writes_.erase(pending->entry.sequence_id);
        sync_cv_.notify_all();
        return;
    }

    persisted_sequences_.insert(pending->entry.sequence_id);
    ErrorCode latest_err = AdvanceCommittedLatestUnlocked(ctx);
    if (latest_err != ErrorCode::OK) {
        pending_writes_.push_front(pending);
        async_cv_.notify_one();
    }
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
                   << ", start_sequence_id=" << start_sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }
    if (latest_reply->type == REDIS_REPLY_STRING &&
        !ParseUint64(std::string(latest_reply->str, latest_reply->len),
                     latest_sequence_id)) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: invalid latest value";
        return ErrorCode::INTERNAL_ERROR;
    }

    uint64_t trimmed_sequence_id = 0;
    RedisReplyPtr trimmed_reply((redisReply*)redisCommand(
        ctx_, "GET %b", trimmed_key_.data(), trimmed_key_.size()));
    if (!trimmed_reply || trimmed_reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: GET trimmed failed"
                   << ", start_sequence_id=" << start_sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }
    if (trimmed_reply->type == REDIS_REPLY_STRING &&
        !ParseUint64(std::string(trimmed_reply->str, trimmed_reply->len),
                     trimmed_sequence_id)) {
        LOG(ERROR) << "RedisOpLogStore::ReadOpLogSince: invalid trimmed value";
        return ErrorCode::INTERNAL_ERROR;
    }

    // TODO(P2P HA): Do not silently advance a lagging standby past the trim
    // horizon. Detect start_sequence_id < trimmed_sequence_id and handle it
    // together with standby snapshot/bootstrap recovery.
    const uint64_t effective_start =
        std::max(start_sequence_id, trimmed_sequence_id);
    if (effective_start >= latest_sequence_id) {
        return ErrorCode::OK;
    }

    entries.reserve(limit);
    uint64_t next_sequence_id = effective_start + 1;
    while (next_sequence_id <= latest_sequence_id && entries.size() < limit) {
        const uint64_t remaining = latest_sequence_id - next_sequence_id + 1;
        const size_t batch_size = static_cast<size_t>(std::min<uint64_t>(
            remaining, static_cast<uint64_t>(limit - entries.size())));
        const uint64_t batch_start = next_sequence_id;

        for (size_t i = 0; i < batch_size; ++i) {
            const uint64_t sequence_id = batch_start + i;
            const std::string entry_key = EntryKey(sequence_id);
            if (redisAppendCommand(ctx_, "GET %b", entry_key.data(),
                                   entry_key.size()) != REDIS_OK) {
                LOG(ERROR)
                    << "RedisOpLogStore::ReadOpLogSince: append GET failed"
                    << ", sequence_id=" << sequence_id;
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
                    << ", sequence_id=" << sequence_id;
                redisFree(ctx_);
                ctx_ = nullptr;
                entries.clear();
                return ErrorCode::INTERNAL_ERROR;
            }
            RedisReplyPtr entry_reply(raw_reply);
            if (entry_reply->type == REDIS_REPLY_NIL) {
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

// Recovery/admin operation inherited from OpLogStore. It must not run
// concurrently with normal writes.
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
        LOG(ERROR) << "RedisOpLogStore::UpdateLatestSequenceId: SET failed";
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
        LOG(ERROR) << "RedisOpLogStore::RecordSnapshotSequenceId: SET failed";
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
                << "RedisOpLogStore::CleanupOpLogBefore: GET latest failed";
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
                << "RedisOpLogStore::CleanupOpLogBefore: GET trimmed failed";
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
                    << ", sequence_id=" << sequence_id;
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
                           << ", sequence_id=" << sequence_id;
                redisFree(ctx_);
                ctx_ = nullptr;
                return ErrorCode::INTERNAL_ERROR;
            }
            RedisReplyPtr del_reply(raw_reply);
            if (del_reply->type == REDIS_REPLY_ERROR) {
                LOG(ERROR) << "RedisOpLogStore::CleanupOpLogBefore: DEL error"
                           << ", sequence_id=" << sequence_id << ", error="
                           << (del_reply->str ? del_reply->str : "null");
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
                << ", trimmed_sequence_id=" << batch_end;
            return ErrorCode::INTERNAL_ERROR;
        }
    }
}

std::unique_ptr<OpLogChangeNotifier> RedisOpLogStore::CreateChangeNotifier(
    const std::string& /*cluster_id*/) {
    return std::make_unique<PollingOpLogChangeNotifier>(this,
                                                        poll_interval_ms_);
}

}  // namespace mooncake

#endif  // STORE_USE_REDIS

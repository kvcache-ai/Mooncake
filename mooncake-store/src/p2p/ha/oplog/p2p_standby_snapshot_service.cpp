#include "p2p/ha/oplog/p2p_standby_snapshot_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <csignal>

#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "p2p/ha/oplog/p2p_hot_standby_service.h"
#ifdef STORE_USE_REDIS
#include "p2p/ha/redis_util.h"
#endif

namespace mooncake {

namespace {

constexpr auto kSnapshotSessionIdleTimeout = std::chrono::minutes(5);
constexpr auto kSnapshotSessionCleanupInterval = std::chrono::minutes(1);

}  // namespace

P2PStandbySnapshotService::P2PStandbySnapshotService(
    P2PHotStandbyService* standby)
    : standby_(standby), cleanup_thread_([this] { CleanupLoop(); }) {}

P2PStandbySnapshotService::~P2PStandbySnapshotService() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    cleanup_cv_.notify_all();
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
}

BeginStandbySnapshotResponse P2PStandbySnapshotService::BeginSnapshot(
    const BeginStandbySnapshotRequest& request) {
    BeginStandbySnapshotResponse response;
    const bool has_standby = standby_ != nullptr;
    const std::string local_cluster_id =
        has_standby ? standby_->GetClusterId() : "";
    const bool snapshot_ready = has_standby &&
                                request.cluster_id == local_cluster_id &&
                                standby_->IsReadyForSnapshot();
    if (!snapshot_ready) {
        LOG(WARNING) << "Standby snapshot: BeginSnapshot rejected"
                     << ", request_cluster_id=" << request.cluster_id
                     << ", local_cluster_id=" << local_cluster_id
                     << ", has_standby=" << has_standby;
        response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        return response;
    }

    // NOTE: This snapshot is a bootstrap seed, not a consistent image of the
    // source metadata at baseline_sequence_id. The source standby may keep
    // applying OpLogs while chunks are fetched; entries deleted during the
    // fetch can be absent from the restored state, while newly-created keys
    // are not added unless they were already in the captured key set.
    // Correctness depends on idempotent OpLog replay, and the restored node
    // must catch up from baseline_sequence_id to converge to the final state.
    Session session;
    // Snapshot chunks are read from the live metadata store after the baseline
    // sequence ID is captured, so concurrent updates may be partially
    // reflected. This is intentional: P2P metadata OpLog operations are
    // designed to be idempotent, and replay from baseline_sequence_id + 1
    // reconciles the snapshot for the asynchronous HA consistency model.
    session.baseline_sequence_id = standby_->GetLatestAppliedSequenceId();
    auto* store = standby_->GetMetadataStore();
    session.object_keys = store->ListObjectKeys();
    session.client_ids = store->ListClientIds();
    session.last_access = std::chrono::steady_clock::now();

    std::lock_guard<std::mutex> lock(mutex_);
    CleanupExpiredSessionsLocked();
    if (sessions_.size() >= kMaxStandbySnapshotSessions) {
        LOG(WARNING) << "Standby snapshot: BeginSnapshot rejected, too many "
                        "active sessions"
                     << ", active_sessions=" << sessions_.size()
                     << ", max_sessions=" << kMaxStandbySnapshotSessions;
        response.error_code = toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        return response;
    }
    response.session_id = std::to_string(next_session_id_++);
    response.baseline_sequence_id = session.baseline_sequence_id;
    response.object_count = session.object_keys.size();
    response.client_count = session.client_ids.size();
    sessions_.emplace(response.session_id, std::move(session));
    LOG(INFO) << "Standby snapshot: session started"
              << ", session_id=" << response.session_id
              << ", baseline_sequence_id=" << response.baseline_sequence_id
              << ", objects=" << response.object_count
              << ", clients=" << response.client_count;
    return response;
}

StandbySnapshotChunkResponse P2PStandbySnapshotService::GetSnapshotChunk(
    const StandbySnapshotChunkRequest& request) {
    StandbySnapshotChunkResponse response;
    if (!standby_ || request.limit == 0 ||
        request.limit > kMaxStandbySnapshotChunkSize) {
        LOG(WARNING) << "Standby snapshot: GetSnapshotChunk rejected, invalid "
                        "request"
                     << ", session_id=" << request.session_id
                     << ", has_standby=" << (standby_ != nullptr)
                     << ", limit=" << request.limit
                     << ", max_limit=" << kMaxStandbySnapshotChunkSize;
        response.error_code = toInt(ErrorCode::INVALID_PARAMS);
        return response;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    CleanupExpiredSessionsLocked();
    auto session_it = sessions_.find(request.session_id);
    if (session_it == sessions_.end()) {
        LOG(WARNING) << "Standby snapshot: GetSnapshotChunk rejected, unknown "
                        "session"
                     << ", session_id=" << request.session_id
                     << ", active_sessions=" << sessions_.size();
        response.error_code = toInt(ErrorCode::INVALID_PARAMS);
        return response;
    }

    auto& session = session_it->second;
    if (request.object_offset > session.object_keys.size() ||
        request.client_offset > session.client_ids.size()) {
        LOG(WARNING) << "Standby snapshot: GetSnapshotChunk rejected, invalid "
                        "offsets"
                     << ", session_id=" << request.session_id
                     << ", object_offset=" << request.object_offset
                     << ", object_count=" << session.object_keys.size()
                     << ", client_offset=" << request.client_offset
                     << ", client_count=" << session.client_ids.size();
        response.error_code = toInt(ErrorCode::INVALID_PARAMS);
        return response;
    }
    session.last_access = std::chrono::steady_clock::now();
    const size_t object_offset = static_cast<size_t>(request.object_offset);
    const size_t client_offset = static_cast<size_t>(request.client_offset);
    const size_t object_end =
        object_offset +
        std::min<size_t>(request.limit,
                         session.object_keys.size() - object_offset);
    for (size_t i = object_offset; i < object_end; ++i) {
        auto metadata =
            standby_->GetMetadataStore()->GetMetadata(session.object_keys[i]);
        if (metadata) {
            response.objects.push_back(
                {session.object_keys[i], std::move(*metadata)});
        }
    }
    response.next_object_offset = object_end;

    const size_t client_end =
        client_offset +
        std::min<size_t>(request.limit,
                         session.client_ids.size() - client_offset);
    for (size_t i = client_offset; i < client_end; ++i) {
        auto info =
            standby_->GetMetadataStore()->GetClientInfo(session.client_ids[i]);
        if (info) {
            response.clients.push_back(
                {session.client_ids[i], std::move(*info)});
        }
    }
    response.next_client_offset = client_end;
    response.done = object_end == session.object_keys.size() &&
                    client_end == session.client_ids.size();
    return response;
}

int32_t P2PStandbySnapshotService::EndSnapshot(
    const EndStandbySnapshotRequest& request) {
    std::lock_guard<std::mutex> lock(mutex_);
    CleanupExpiredSessionsLocked();
    return sessions_.erase(request.session_id) == 1
               ? toInt(ErrorCode::OK)
               : toInt(ErrorCode::INVALID_PARAMS);
}

void P2PStandbySnapshotService::CleanupExpiredSessionsLocked() {
    const auto now = std::chrono::steady_clock::now();
    for (auto it = sessions_.begin(); it != sessions_.end();) {
        if (now - it->second.last_access > kSnapshotSessionIdleTimeout) {
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }
}

void P2PStandbySnapshotService::CleanupLoop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopping_) {
        cleanup_cv_.wait_for(lock, kSnapshotSessionCleanupInterval,
                             [this] { return stopping_; });
        if (!stopping_) {
            CleanupExpiredSessionsLocked();
        }
    }
}

namespace {

bool ParseEndpoint(const std::string& endpoint, std::string& host,
                   std::string& port) {
    auto separator = endpoint.rfind(':');
    if (separator == std::string::npos || separator == 0 ||
        separator + 1 == endpoint.size()) {
        return false;
    }
    host = endpoint.substr(0, separator);
    port = endpoint.substr(separator + 1);
    return true;
}

}  // namespace

ErrorCode P2PStandbySnapshotClient::Bootstrap(const std::string& endpoint,
                                              const std::string& cluster_id,
                                              P2PStandbyMetadataStore* target,
                                              uint64_t& baseline_sequence_id,
                                              uint32_t chunk_size) {
    if (!target || chunk_size == 0 ||
        chunk_size > kMaxStandbySnapshotChunkSize) {
        LOG(WARNING) << "Standby snapshot: bootstrap rejected, invalid "
                        "request"
                     << ", endpoint=" << endpoint
                     << ", has_target=" << (target != nullptr)
                     << ", chunk_size=" << chunk_size
                     << ", max_chunk_size=" << kMaxStandbySnapshotChunkSize;
        return ErrorCode::INVALID_PARAMS;
    }

    std::string host;
    std::string port;
    if (!ParseEndpoint(endpoint, host, port)) {
        LOG(WARNING) << "Standby snapshot: bootstrap rejected, invalid "
                        "endpoint"
                     << ", endpoint=" << endpoint;
        return ErrorCode::INVALID_PARAMS;
    }

    coro_rpc::coro_rpc_client client;
    auto connect_error = async_simple::coro::syncAwait(
        client.connect(host, port, std::chrono::seconds(5)));
    if (connect_error) {
        LOG(WARNING) << "Standby snapshot: connect failed"
                     << ", endpoint=" << endpoint
                     << ", error=" << connect_error.message();
        return ErrorCode::RPC_FAIL;
    }

    auto begin = async_simple::coro::syncAwait(
        client.call_for<&P2PStandbySnapshotService::BeginSnapshot>(
            std::chrono::seconds(10), BeginStandbySnapshotRequest{cluster_id}));
    if (!begin || fromInt(begin->error_code) != ErrorCode::OK) {
        LOG(WARNING) << "Standby snapshot: BeginSnapshot RPC failed"
                     << ", endpoint=" << endpoint << ", error="
                     << (begin ? toString(fromInt(begin->error_code))
                               : toString(ErrorCode::RPC_FAIL));
        return begin ? fromInt(begin->error_code) : ErrorCode::RPC_FAIL;
    }

    const std::string session_id = begin->session_id;
    uint64_t object_offset = 0;
    uint64_t client_offset = 0;
    uint64_t chunk_count = 0;
    const auto started_at = std::chrono::steady_clock::now();
    LOG(INFO) << "Standby snapshot: bootstrap started"
              << ", endpoint=" << endpoint << ", session_id=" << session_id
              << ", baseline_sequence_id=" << begin->baseline_sequence_id
              << ", objects=" << begin->object_count
              << ", clients=" << begin->client_count;
    target->RemoveAllMetadata();

    ErrorCode result = ErrorCode::OK;
    while (true) {
        StandbySnapshotChunkRequest request;
        request.session_id = session_id;
        request.object_offset = object_offset;
        request.client_offset = client_offset;
        request.limit = chunk_size;
        auto chunk = async_simple::coro::syncAwait(
            client.call_for<&P2PStandbySnapshotService::GetSnapshotChunk>(
                std::chrono::seconds(30), request));
        if (!chunk || fromInt(chunk->error_code) != ErrorCode::OK) {
            result = chunk ? fromInt(chunk->error_code) : ErrorCode::RPC_FAIL;
            LOG(WARNING) << "Standby snapshot: GetSnapshotChunk RPC failed"
                         << ", endpoint=" << endpoint
                         << ", session_id=" << session_id
                         << ", object_offset=" << object_offset
                         << ", client_offset=" << client_offset
                         << ", limit=" << request.limit
                         << ", error=" << toString(result);
            break;
        }
        for (const auto& record : chunk->clients) {
            target->RegisterClient(record.client_id, record.info.ip_address,
                                   record.info.rpc_port, record.info.segments);
        }
        for (const auto& record : chunk->objects) {
            target->RestoreMetadata(record.key, record.metadata);
        }
        object_offset = chunk->next_object_offset;
        client_offset = chunk->next_client_offset;
        ++chunk_count;
        if (chunk->done || chunk_count % 100 == 0) {
            LOG(INFO) << "Standby snapshot: bootstrap progress"
                      << ", endpoint=" << endpoint
                      << ", session_id=" << session_id
                      << ", objects=" << object_offset << "/"
                      << begin->object_count << ", clients=" << client_offset
                      << "/" << begin->client_count;
        }
        if (chunk->done) {
            baseline_sequence_id = begin->baseline_sequence_id;
            break;
        }
    }

    if (result != ErrorCode::OK) {
        LOG(ERROR) << "Standby snapshot: bootstrap failed"
                   << ", endpoint=" << endpoint << ", session_id=" << session_id
                   << ", object_offset=" << object_offset
                   << ", client_offset=" << client_offset
                   << ", error=" << toString(result);
        target->RemoveAllMetadata();
    } else {
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - started_at);
        LOG(INFO) << "Standby snapshot: bootstrap completed"
                  << ", endpoint=" << endpoint << ", session_id=" << session_id
                  << ", baseline_sequence_id=" << baseline_sequence_id
                  << ", chunks=" << chunk_count
                  << ", elapsed_ms=" << elapsed.count();
    }

    // EndSnapshot is best-effort; the server-side CleanupLoop removes idle
    // sessions by timeout if this cleanup RPC fails.
    auto end = async_simple::coro::syncAwait(
        client.call_for<&P2PStandbySnapshotService::EndSnapshot>(
            std::chrono::seconds(5), EndStandbySnapshotRequest{session_id}));
    const ErrorCode end_result = end ? fromInt(*end) : ErrorCode::RPC_FAIL;
    if (end_result != ErrorCode::OK) {
        LOG(WARNING) << "Standby snapshot: EndSnapshot RPC failed"
                     << ", endpoint=" << endpoint
                     << ", session_id=" << session_id
                     << ", error=" << toString(end_result);
    }
    return result;
}

#ifdef STORE_USE_REDIS
namespace {

std::string SerializeMasterRegistryEntry(
    const RedisMasterRegistryEntry& entry) {
    return entry.master_endpoint + "\n" + entry.snapshot_endpoint + "\n" +
           entry.role + "\n" + (entry.snapshot_ready ? "1" : "0") + "\n" +
           std::to_string(entry.applied_sequence_id);
}

bool ParseMasterRegistryEntry(const std::string& instance_id,
                              const std::string& value,
                              RedisMasterRegistryEntry& entry) {
    std::vector<std::string> fields;
    size_t begin = 0;
    while (true) {
        const auto separator = value.find('\n', begin);
        if (separator == std::string::npos) {
            fields.emplace_back(value.substr(begin));
            break;
        }
        fields.emplace_back(value.substr(begin, separator - begin));
        begin = separator + 1;
    }
    if ((fields.size() != 4 && fields.size() != 5) ||
        (fields[3] != "0" && fields[3] != "1")) {
        LOG(WARNING) << "Redis Master registry entry has invalid metadata"
                     << ", instance_id=" << instance_id
                     << ", field_count=" << fields.size()
                     << ", snapshot_ready_field="
                     << (fields.size() > 3 ? fields[3] : "");
        return false;
    }
    uint64_t applied_sequence_id = 0;
    if (fields.size() == 5) {
        const auto [end, error] = std::from_chars(
            fields[4].data(), fields[4].data() + fields[4].size(),
            applied_sequence_id);
        if (error != std::errc() ||
            end != fields[4].data() + fields[4].size()) {
            LOG(WARNING)
                << "Redis Master registry entry has invalid applied sequence"
                << ", instance_id=" << instance_id
                << ", applied_sequence_id_field=" << fields[4];
            return false;
        }
    }
    entry.instance_id = instance_id;
    entry.master_endpoint = std::move(fields[0]);
    entry.snapshot_endpoint = std::move(fields[1]);
    entry.role = std::move(fields[2]);
    entry.snapshot_ready = fields[3] == "1";
    entry.applied_sequence_id = applied_sequence_id;
    return true;
}

}  // namespace

RedisMasterRegistry::RedisMasterRegistry(std::string cluster_id,
                                         std::string redis_endpoint,
                                         std::string username,
                                         std::string password, int db_index)
    : heartbeat_key_("mooncake:{" + cluster_id + "}:masters:heartbeat"),
      metadata_key_("mooncake:{" + cluster_id + "}:masters:metadata"),
      redis_endpoint_(std::move(redis_endpoint)),
      username_(std::move(username)),
      password_(std::move(password)),
      db_index_(db_index) {}

ErrorCode RedisMasterRegistry::Refresh(const RedisMasterRegistryEntry& entry) {
    if (entry.instance_id.empty() || entry.master_endpoint.empty()) {
        LOG(ERROR) << "Redis Master registry refresh got invalid entry"
                   << ", instance_id=" << entry.instance_id
                   << ", master_endpoint=" << entry.master_endpoint
                   << ", snapshot_endpoint=" << entry.snapshot_endpoint
                   << ", role=" << entry.role;
        return ErrorCode::INVALID_PARAMS;
    }
    std::unique_ptr<redisContext, decltype(&redisFree)> ctx(
        RedisUtil::CreateConnection(redis_endpoint_, username_, password_,
                                    db_index_),
        &redisFree);
    if (!ctx) {
        LOG(ERROR) << "Redis Master registry refresh failed to connect"
                   << ", instance_id=" << entry.instance_id
                   << ", role=" << entry.role;
        return ErrorCode::INTERNAL_ERROR;
    }
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    const std::string metadata = SerializeMasterRegistryEntry(entry);
    static constexpr const char* kRefreshScript =
        "redis.call('HSET', KEYS[2], ARGV[1], ARGV[3]); "
        "redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1]); "
        "return 1";
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx.get(), "EVAL %s 2 %b %b %b %lld %b", kRefreshScript,
        heartbeat_key_.data(), heartbeat_key_.size(), metadata_key_.data(),
        metadata_key_.size(), entry.instance_id.data(),
        entry.instance_id.size(), static_cast<long long>(now_ms),
        metadata.data(), metadata.size()));
    return reply && reply->type != REDIS_REPLY_ERROR
               ? ErrorCode::OK
               : ErrorCode::INTERNAL_ERROR;
}

ErrorCode RedisMasterRegistry::Remove(const std::string& instance_id) {
    if (instance_id.empty()) {
        LOG(ERROR) << "Redis Master registry remove got empty instance_id";
        return ErrorCode::INVALID_PARAMS;
    }
    std::unique_ptr<redisContext, decltype(&redisFree)> ctx(
        RedisUtil::CreateConnection(redis_endpoint_, username_, password_,
                                    db_index_),
        &redisFree);
    if (!ctx) {
        LOG(ERROR) << "Redis Master registry remove failed to connect"
                   << ", instance_id=" << instance_id;
        return ErrorCode::INTERNAL_ERROR;
    }
    static constexpr const char* kRemoveScript =
        "redis.call('ZREM', KEYS[1], ARGV[1]); "
        "redis.call('HDEL', KEYS[2], ARGV[1]); "
        "return 1";
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx.get(), "EVAL %s 2 %b %b %b", kRemoveScript, heartbeat_key_.data(),
        heartbeat_key_.size(), metadata_key_.data(), metadata_key_.size(),
        instance_id.data(), instance_id.size()));
    return reply && reply->type != REDIS_REPLY_ERROR
               ? ErrorCode::OK
               : ErrorCode::INTERNAL_ERROR;
}

ErrorCode RedisMasterRegistry::DiscoverAlive(
    std::chrono::seconds ttl, std::vector<RedisMasterRegistryEntry>& entries) {
    entries.clear();
    std::unique_ptr<redisContext, decltype(&redisFree)> ctx(
        RedisUtil::CreateConnection(redis_endpoint_, username_, password_,
                                    db_index_),
        &redisFree);
    if (!ctx) {
        LOG(ERROR) << "Redis Master registry discover failed to connect"
                   << ", ttl_sec=" << ttl.count();
        return ErrorCode::INTERNAL_ERROR;
    }
    const auto cutoff_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() - ttl)
            .count();
    // Entries exactly at cutoff are conservatively treated as stale.
    static constexpr const char* kDiscoverScript =
        "local stale = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1]); "
        "for _, id in ipairs(stale) do redis.call('HDEL', KEYS[2], id); end; "
        "redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1]); "
        "local alive = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], '+inf'); "
        "local result = {}; "
        "for _, id in ipairs(alive) do "
        "local metadata = redis.call('HGET', KEYS[2], id); "
        "if metadata then table.insert(result, id); "
        "table.insert(result, metadata); end; end; "
        "return result";
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx.get(), "EVAL %s 2 %b %b %lld", kDiscoverScript,
        heartbeat_key_.data(), heartbeat_key_.size(), metadata_key_.data(),
        metadata_key_.size(), static_cast<long long>(cutoff_ms)));
    if (!reply || reply->type != REDIS_REPLY_ARRAY ||
        reply->elements % 2 != 0) {
        LOG(ERROR) << "Redis Master registry discover got invalid Redis reply"
                   << ", reply_type=" << (reply ? reply->type : -1)
                   << ", element_count=" << (reply ? reply->elements : 0);
        return ErrorCode::INTERNAL_ERROR;
    }
    for (size_t i = 0; i < reply->elements; i += 2) {
        auto* id = reply->element[i];
        auto* metadata = reply->element[i + 1];
        if (!id || id->type != REDIS_REPLY_STRING || !metadata ||
            metadata->type != REDIS_REPLY_STRING) {
            continue;
        }
        RedisMasterRegistryEntry entry;
        if (ParseMasterRegistryEntry(std::string(id->str, id->len),
                                     std::string(metadata->str, metadata->len),
                                     entry)) {
            entries.push_back(std::move(entry));
        }
    }
    return ErrorCode::OK;
}

RedisMasterRegistryHeartbeat::RedisMasterRegistryHeartbeat(
    std::unique_ptr<RedisMasterRegistry> registry,
    RedisMasterRegistryEntry entry, std::chrono::seconds interval)
    : registry_(std::move(registry)),
      entry_(std::move(entry)),
      interval_(interval) {}

RedisMasterRegistryHeartbeat::~RedisMasterRegistryHeartbeat() { Stop(); }

ErrorCode RedisMasterRegistryHeartbeat::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (thread_.joinable()) {
        return ErrorCode::OK;
    }
    stopping_ = false;
    auto err = registry_->Refresh(entry_);
    thread_ = std::thread(&RedisMasterRegistryHeartbeat::Run, this);
    if (err == ErrorCode::OK) {
        LOG(INFO) << "Redis Master registered"
                  << ", instance_id=" << entry_.instance_id
                  << ", master_endpoint=" << entry_.master_endpoint
                  << ", snapshot_endpoint=" << entry_.snapshot_endpoint
                  << ", role=" << entry_.role;
    } else {
        LOG(ERROR) << "Redis Master initial registration failed"
                   << ", instance_id=" << entry_.instance_id
                   << ", master_endpoint=" << entry_.master_endpoint
                   << ", snapshot_endpoint=" << entry_.snapshot_endpoint
                   << ", role=" << entry_.role << ", error=" << toString(err);
    }
    return err;
}

void RedisMasterRegistryHeartbeat::UpdateRole(std::string role,
                                              bool snapshot_ready) {
    std::lock_guard<std::mutex> lock(mutex_);
    entry_.role = std::move(role);
    entry_.snapshot_ready = snapshot_ready;
    refresh_requested_ = true;
    cv_.notify_all();
}

void RedisMasterRegistryHeartbeat::SetAppliedSequenceProvider(
    std::function<uint64_t()> provider) {
    std::unique_lock<std::mutex> lock(mutex_);
    applied_sequence_provider_ = std::move(provider);
    refresh_requested_ = true;
    cv_.notify_all();
    cv_.wait(lock, [this] { return provider_in_flight_ == 0; });
}

void RedisMasterRegistryHeartbeat::SetSnapshotReadyProvider(
    std::function<bool()> provider) {
    std::unique_lock<std::mutex> lock(mutex_);
    snapshot_ready_provider_ = std::move(provider);
    refresh_requested_ = true;
    cv_.notify_all();
    cv_.wait(lock, [this] { return provider_in_flight_ == 0; });
}

void RedisMasterRegistryHeartbeat::Stop() {
    std::string instance_id;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!thread_.joinable()) {
            return;
        }
        stopping_ = true;
        instance_id = entry_.instance_id;
    }
    cv_.notify_all();
    thread_.join();
    (void)registry_->Remove(instance_id);
}

void RedisMasterRegistryHeartbeat::Run() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopping_) {
        cv_.wait_for(lock, interval_,
                     [this] { return stopping_ || refresh_requested_; });
        if (stopping_) {
            break;
        }
        refresh_requested_ = false;

        auto entry = entry_;
        std::function<uint64_t()> applied_sequence_provider;
        std::function<bool()> snapshot_ready_provider;
        if (entry.role == "standby") {
            applied_sequence_provider = applied_sequence_provider_;
            snapshot_ready_provider = snapshot_ready_provider_;
        }
        bool has_provider =
            applied_sequence_provider || snapshot_ready_provider;
        if (has_provider) {
            ++provider_in_flight_;
        }

        lock.unlock();
        if (applied_sequence_provider) {
            entry.applied_sequence_id = applied_sequence_provider();
        }
        if (snapshot_ready_provider) {
            entry.snapshot_ready = snapshot_ready_provider();
        }
        lock.lock();

        if (has_provider) {
            --provider_in_flight_;
            cv_.notify_all();
        }
        if (stopping_) {
            break;
        }
        if (refresh_requested_) {
            continue;
        }
        entry_ = entry;

        lock.unlock();
        if (registry_->Refresh(entry) != ErrorCode::OK) {
            LOG(WARNING) << "Redis Master registry heartbeat failed"
                         << ", instance_id=" << entry.instance_id
                         << ", role=" << entry.role;
        }
        lock.lock();
    }
}
#endif

}  // namespace mooncake

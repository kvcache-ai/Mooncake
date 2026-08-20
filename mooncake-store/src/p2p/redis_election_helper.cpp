#ifdef STORE_USE_REDIS

#include "redis_election_helper.h"

#include <glog/logging.h>

#include <chrono>
#include <cerrno>
#include <cstring>
#include <functional>
#include <iomanip>
#include <json/json.h>
#include <poll.h>
#include <random>
#include <sstream>

#include "p2p/p2p_ha_metric_manager.h"

namespace mooncake {

namespace {

class ScopeGuard {
   public:
    explicit ScopeGuard(std::function<void()> callback)
        : callback_(std::move(callback)) {}
    ~ScopeGuard() {
        if (callback_) {
            callback_();
        }
    }

    ScopeGuard(const ScopeGuard&) = delete;
    ScopeGuard& operator=(const ScopeGuard&) = delete;

   private:
    std::function<void()> callback_;
};

std::string GenerateCandidateId() {
    std::random_device random;
    std::mt19937_64 generator(random());
    std::ostringstream stream;
    stream << std::hex << std::setfill('0') << std::setw(16) << generator()
           << std::setw(16) << generator();
    return stream.str();
}

bool ParseElectionValue(const std::string& value, std::string& address,
                        ViewVersionId& epoch, std::string* candidate_id) {
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::istringstream stream(value);
    std::string errors;
    if (!Json::parseFromStream(builder, stream, &root, &errors)) {
        LOG(ERROR) << "ParseLeaderValue: JSON parse failed: " << errors;
        return false;
    }

    if (!root.isMember("address") || !root["address"].isString() ||
        !root.isMember("epoch") || !root["epoch"].isInt64()) {
        return false;
    }

    address = root["address"].asString();
    epoch = root["epoch"].asInt64();
    if (candidate_id) {
        if (!root.isMember("candidate_id") ||
            !root["candidate_id"].isString()) {
            return false;
        }
        *candidate_id = root["candidate_id"].asString();
    }
    return true;
}

}  // namespace

// ============================================================
// Construction / Destruction
// ============================================================

RedisElectionHelper::RedisElectionHelper(const std::string& cluster_id,
                                         const std::string& redis_endpoint,
                                         const std::string& password,
                                         int db_index, int ttl_sec,
                                         int heartbeat_interval_sec,
                                         const std::string& username)
    : redis_endpoint_(redis_endpoint),
      username_(username),
      password_(password),
      db_index_(db_index),
      ttl_sec_(ttl_sec),
      heartbeat_interval_sec_(heartbeat_interval_sec),
      cluster_id_(cluster_id) {
    std::string cid = cluster_id;
    if (!cid.empty() && cid.back() != '/') {
        cid += '/';
    }
    // Use {cid} with braces as hash tag for Redis cluster slot affinity
    master_view_key_ = "mooncake:{" + cid + "}master_view";
    master_epoch_key_ = "mooncake:{" + cid + "}master_epoch";
    leader_event_channel_ = "mooncake:" + cid + "leader_event";
    LOG(INFO) << "RedisElectionHelper created, master_view_key="
              << master_view_key_ << " epoch_key=" << master_epoch_key_
              << " channel=" << leader_event_channel_ << " ttl=" << ttl_sec_
              << "s"
              << " heartbeat=" << heartbeat_interval_sec_ << "s";
}

RedisElectionHelper::~RedisElectionHelper() {
    Shutdown();
    {
        std::lock_guard<std::mutex> lock(subscribe_mutex_);
        if (subscribe_ctx_) {
            redisFree(subscribe_ctx_);
            subscribe_ctx_ = nullptr;
        }
    }
    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        if (election_ctx_) {
            redisFree(election_ctx_);
            election_ctx_ = nullptr;
        }
    }
}

// ============================================================
// CreateConnection — common logic for Connect / polling
// ============================================================

redisContext* RedisElectionHelper::CreateConnection() {
    return RedisUtil::CreateConnection(redis_endpoint_, username_, password_,
                                       db_index_, connect_timeout_ms_,
                                       command_timeout_ms_);
}

// ============================================================
// Connect
// ============================================================

ErrorCode RedisElectionHelper::Connect() {
    std::unique_lock<std::mutex> operation_lock(operation_mutex_);
    operation_cv_.wait(operation_lock, [this] {
        return shutting_down_ || active_blocking_operations_ == 0;
    });
    if (shutting_down_) {
        LOG(ERROR) << "Connect: helper is shutting down";
        return ErrorCode::INTERNAL_ERROR;
    }

    cancel_election_ = false;
    cancel_keep_alive_ = false;

    // Election connection
    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        if (election_ctx_) {
            redisFree(election_ctx_);
            election_ctx_ = nullptr;
        }
        election_ctx_ = CreateConnection();
        if (!election_ctx_) {
            LOG(ERROR)
                << "Connect: failed to create election connection to Redis at "
                << redis_endpoint_;
            return ErrorCode::INTERNAL_ERROR;
        }
    }

    // Subscribe connection (separate, as SUBSCRIBE changes connection mode).
    {
        std::lock_guard<std::mutex> lock(subscribe_mutex_);
        if (!ReconnectSubscribeLocked(/*record_metric=*/false)) {
            // Non-fatal: WatchLeader will fall back to polling when
            // subscribe_ctx_ is null.
            LOG(ERROR) << "Failed to create subscribe connection to Redis at "
                       << redis_endpoint_;
        }
    }

    LOG(INFO) << "Connected to Redis";
    return ErrorCode::OK;
}

// ============================================================
// ElectLeader — blocks until this node wins election
// ============================================================

void RedisElectionHelper::ElectLeader(const std::string& master_address,
                                      ViewVersionId& version, int& lease_id) {
    if (!BeginBlockingOperation()) {
        return;
    }
    ScopeGuard operation([this] { EndBlockingOperation(); });
    const auto election_start = std::chrono::steady_clock::now();
    while (!cancel_election_) {
        P2PHAMetricManager::instance().inc_election_attempts();
        bool connected = false;
        {
            std::lock_guard<std::mutex> lock(election_mutex_);
            if (!election_ctx_) {
                election_ctx_ = CreateConnection();
                if (!election_ctx_) {
                    P2PHAMetricManager::instance().inc_election_failures();
                    LOG(ERROR) << "ElectLeader: connect failed, retry in 1s";
                }
            }
            connected = (election_ctx_ != nullptr);
        }
        if (!connected) {
            WaitForElectionCancellation(std::chrono::seconds(1));
            continue;
        }

        ElectionAttemptResult attempt = ElectionAttemptResult::ERROR;
        std::string existing_value;
        {
            std::lock_guard<std::mutex> lock(election_mutex_);
            attempt = TryElectOnce(master_address, version, existing_value);
        }
        if (attempt == ElectionAttemptResult::ERROR) {
            WaitForElectionCancellation(std::chrono::seconds(1));
            continue;
        }
        if (attempt == ElectionAttemptResult::ELECTED) {
            {
                std::lock_guard<std::mutex> lock(election_mutex_);
                PublishLeaderEvent(master_address, version);
            }
            P2PHAMetricManager::instance().set_election_is_leader(true);
            P2PHAMetricManager::instance().observe_election_duration_ms(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - election_start)
                    .count());
            lease_id = next_lease_id_++;
            LOG(INFO) << "ElectLeader: elected as leader, epoch=" << version
                      << " lease_id=" << lease_id;
            return;
        }

        std::string current_addr;
        ViewVersionId current_epoch = 0;
        if (ParseLeaderValue(existing_value, current_addr, current_epoch)) {
            LOG(INFO) << "ElectLeader: current leader=" << current_addr
                      << " epoch=" << current_epoch << ", waiting...";
        } else {
            LOG(WARNING) << "ElectLeader: leader key exists but unparsable: "
                         << existing_value << ", waiting...";
        }
        WatchLeader();  // Blocks until key expires or an election event arrives
    }
}

RedisElectionHelper::ElectionAttemptResult RedisElectionHelper::TryElectOnce(
    const std::string& master_address, ViewVersionId& out_epoch,
    std::string& existing_value) {
    // Caller must hold election_mutex_
    const char* election_script =
        "local current = redis.call('GET', KEYS[1]) "
        "if current then return {0, current} end "
        "local epoch = redis.call('INCR', KEYS[2]) "
        "local value = cjson.encode({address=ARGV[1], epoch=epoch, "
        "  ts=tonumber(ARGV[2]), ttl=tonumber(ARGV[3]), "
        "  candidate_id=ARGV[4]}) "
        "redis.call('SET', KEYS[1], value, 'EX', ARGV[3]) "
        "return {1, tostring(epoch), value}";

    const std::string candidate_id = GenerateCandidateId();
    const auto timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    RedisReplyPtr reply((redisReply*)redisCommand(
        election_ctx_, "EVAL %s 2 %b %b %b %lld %d %b", election_script,
        master_view_key_.data(), master_view_key_.size(),
        master_epoch_key_.data(), master_epoch_key_.size(),
        master_address.data(), master_address.size(),
        static_cast<long long>(timestamp_ms), ttl_sec_, candidate_id.data(),
        candidate_id.size()));

    if (!reply) {
        P2PHAMetricManager::instance().inc_election_failures();

        // The script may have completed before the response was lost. After
        // reconnecting, identify our write by its per-attempt candidate ID.
        if (!Reconnect(election_ctx_)) {
            LOG(WARNING) << "TryElectOnce: election script response was lost "
                            "and reconnect failed";
            return ElectionAttemptResult::ERROR;
        }
        RedisReplyPtr current((redisReply*)redisCommand(
            election_ctx_, "GET %b", master_view_key_.data(),
            master_view_key_.size()));
        if (!current || current->type != REDIS_REPLY_STRING) {
            LOG(WARNING)
                << "TryElectOnce: failed to read the election result after "
                   "reconnect";
            return ElectionAttemptResult::ERROR;
        }
        std::string value(current->str, current->len);
        std::string address;
        std::string stored_candidate_id;
        ViewVersionId epoch = 0;
        if (ParseElectionValue(value, address, epoch, &stored_candidate_id) &&
            stored_candidate_id == candidate_id) {
            out_epoch = epoch;
            our_value_ = std::move(value);
            LOG(WARNING) << "TryElectOnce: election script response was lost; "
                            "confirmed election after reconnect";
            return ElectionAttemptResult::ELECTED;
        }
        LOG(WARNING) << "TryElectOnce: election result after reconnect does "
                        "not match this attempt";
        return ElectionAttemptResult::ERROR;
    }

    const auto element_type = [&reply](size_t index) {
        if (reply->type != REDIS_REPLY_ARRAY || index >= reply->elements ||
            reply->element[index] == nullptr) {
            return -1;
        }
        return reply->element[index]->type;
    };

    if (reply->type != REDIS_REPLY_ARRAY || reply->elements < 2 ||
        element_type(0) != REDIS_REPLY_INTEGER) {
        P2PHAMetricManager::instance().inc_election_failures();
        LOG(WARNING) << "TryElectOnce: unexpected election script reply"
                     << ", type=" << reply->type
                     << ", elements=" << reply->elements
                     << ", result_type=" << element_type(0);
        return ElectionAttemptResult::ERROR;
    }

    if (reply->element[0]->integer == 0) {
        if (element_type(1) != REDIS_REPLY_STRING) {
            LOG(WARNING)
                << "TryElectOnce: contended election reply has an invalid "
                   "leader value, leader_type="
                << element_type(1);
            return ElectionAttemptResult::ERROR;
        }
        existing_value.assign(reply->element[1]->str, reply->element[1]->len);
        return ElectionAttemptResult::CONTENDED;
    }

    if (reply->elements != 3 || element_type(1) != REDIS_REPLY_STRING ||
        element_type(2) != REDIS_REPLY_STRING) {
        LOG(WARNING) << "TryElectOnce: elected reply has an unexpected format"
                     << ", elements=" << reply->elements
                     << ", epoch_type=" << element_type(1)
                     << ", value_type=" << element_type(2);
        return ElectionAttemptResult::ERROR;
    }
    try {
        out_epoch = std::stoll(
            std::string(reply->element[1]->str, reply->element[1]->len));
    } catch (const std::exception& error) {
        LOG(WARNING) << "TryElectOnce: invalid epoch"
                     << ", length=" << reply->element[1]->len
                     << ", error=" << error.what();
        return ElectionAttemptResult::ERROR;
    }
    our_value_.assign(reply->element[2]->str, reply->element[2]->len);
    return ElectionAttemptResult::ELECTED;
}

// ============================================================
// WatchLeader — wait until leader key expires
// ============================================================

void RedisElectionHelper::WatchLeader() {
    // Fast path: SUBSCRIBE for leader event notification
    if (WatchLeaderSubscribe()) {
        return;
    }

    // Slow path (fallback): pure polling — use a separate connection
    P2PHAMetricManager::instance().inc_election_watch_failures();
    P2PHAMetricManager::instance().inc_election_polling_fallbacks();
    WatchLeaderPolling();
}

bool RedisElectionHelper::WatchLeaderSubscribe() {
    std::lock_guard<std::mutex> subscribe_lock(subscribe_mutex_);
    // Attempt SUBSCRIBE; return true if successful, false to fall back to
    // polling.
    if (!subscribe_ctx_) return false;

    RedisReplyPtr reply((redisReply*)redisCommand(
        subscribe_ctx_, "SUBSCRIBE %b", leader_event_channel_.data(),
        leader_event_channel_.size()));

    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements < 3 ||
        reply->element[0]->type != REDIS_REPLY_STRING ||
        strncmp(reply->element[0]->str, "subscribe", 9) != 0) {
        ReconnectSubscribeLocked();
        return false;  // Fall back to polling
    }
    reply.reset();

    std::atomic<bool> leader_lost{false};
    redisContext* polling_ctx = CreateConnection();
    if (!polling_ctx) {
        LOG(WARNING) << "WatchLeaderSubscribe: failed to create polling "
                        "connection; falling back to pure polling";
        RedisReplyPtr unsub((redisReply*)redisCommand(
            subscribe_ctx_, "UNSUBSCRIBE %b", leader_event_channel_.data(),
            leader_event_channel_.size()));
        return false;
    }

    // Polling thread: check if key still exists periodically.
    std::thread polling_thread([this, &leader_lost, polling_ctx]() {
        auto interval = std::chrono::seconds(ttl_sec_);
        while (!leader_lost && !cancel_election_) {
            RedisReplyPtr r((redisReply*)redisCommand(polling_ctx, "GET %b",
                                                      master_view_key_.data(),
                                                      master_view_key_.size()));
            if (!r) {
                LOG(WARNING) << "WatchLeaderSubscribe: polling GET failed "
                                "(connection error), exiting polling thread";
                break;
            }
            if (r->type == REDIS_REPLY_NIL) {
                leader_lost = true;
                cancel_cv_.notify_all();
                break;
            }

            std::unique_lock<std::mutex> lock(cancel_mutex_);
            cancel_cv_.wait_for(lock, interval, [&] {
                return leader_lost.load() || cancel_election_.load();
            });
        }
    });

    // Wait for socket readability before calling hiredis. This distinguishes
    // an idle subscription from a broken socket without mutating hiredis
    // internal error state and keeps cancellation responsive.
    bool subscribe_failed = false;
    while (!leader_lost && !cancel_election_) {
        pollfd descriptor{subscribe_ctx_->fd, POLLIN, 0};
        int poll_result = ::poll(&descriptor, 1, 200);
        if (poll_result == 0) {
            continue;
        }
        if (poll_result < 0) {
            if (errno == EINTR) {
                continue;
            }
            subscribe_failed = true;
            break;
        }
        if ((descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            subscribe_failed = true;
            break;
        }
        if ((descriptor.revents & POLLIN) == 0) {
            continue;
        }

        redisReply* msg = nullptr;
        if (redisGetReply(subscribe_ctx_, (void**)&msg) != REDIS_OK) {
            subscribe_failed = true;
            break;
        }

        if (msg) {
            RedisReplyPtr msg_guard(msg);
            if (msg_guard->type == REDIS_REPLY_ARRAY &&
                msg_guard->elements >= 3) {
                if (msg_guard->element[0]->type == REDIS_REPLY_STRING &&
                    strncmp(msg_guard->element[0]->str, "message", 7) == 0) {
                    leader_lost = true;
                    cancel_cv_.notify_all();
                }
            }
        }
    }

    // If the loop exited without leader_lost or cancel_election_, the
    // subscribe connection is broken — fall back to pure polling.
    subscribe_failed = subscribe_failed && !leader_lost && !cancel_election_;
    leader_lost = true;  // Signal polling thread to stop
    cancel_cv_.notify_all();
    if (polling_thread.joinable()) {
        polling_thread.join();
    }

    // Clean up polling connection
    if (polling_ctx) {
        redisFree(polling_ctx);
    }

    if (subscribe_failed) {
        ReconnectSubscribeLocked();
        return false;
    }

    // Unsubscribe to restore connection state
    RedisReplyPtr unsub((redisReply*)redisCommand(
        subscribe_ctx_, "UNSUBSCRIBE %b", leader_event_channel_.data(),
        leader_event_channel_.size()));

    return true;
}

void RedisElectionHelper::WatchLeaderPolling() {
    LOG(INFO) << "WatchLeader: using polling fallback (interval=" << ttl_sec_
              << "s)";
    redisContext* polling_ctx = CreateConnection();
    auto interval = std::chrono::seconds(ttl_sec_);
    while (!cancel_election_) {
        if (!polling_ctx) {
            // Connection was never established or previously lost — retry
            polling_ctx = CreateConnection();
        } else {
            RedisReplyPtr reply((redisReply*)redisCommand(
                polling_ctx, "GET %b", master_view_key_.data(),
                master_view_key_.size()));
            if (reply) {
                if (reply->type == REDIS_REPLY_NIL) {
                    redisFree(polling_ctx);
                    return;  // Key expired
                }
            } else {
                // Connection error — reconnect on the next iteration.
                redisFree(polling_ctx);
                polling_ctx = nullptr;
            }
        }

        if (WaitForElectionCancellation(interval)) {
            break;
        }
    }
    if (polling_ctx) redisFree(polling_ctx);
}

// ============================================================
// KeepLeader — renew TTL via Lua script, block until lost
// ============================================================

void RedisElectionHelper::KeepLeader(int lease_id) {
    if (!BeginKeepAliveOperation()) {
        return;
    }
    ScopeGuard operation([this] { EndKeepAliveOperation(); });
    (void)lease_id;  // Reserved for future lease validation
    keep_alive_running_ = true;

    // Lua script: atomically check ownership and renew TTL
    // KEYS[1] = master_view_key
    // ARGV[1] = TTL seconds
    // ARGV[2] = our value (the JSON we wrote)
    const char* renewal_script =
        "local val = redis.call('GET', KEYS[1]) "
        "if val == ARGV[2] then "
        "  redis.call('EXPIRE', KEYS[1], ARGV[1]) "
        "  return 1 "
        "else "
        "  return 0 "
        "end";

    LOG(INFO) << "KeepLeader: starting renewal loop (interval="
              << heartbeat_interval_sec_ << "s)";

    while (keep_alive_running_ && !cancel_keep_alive_) {
        bool renewed = false;
        {
            std::lock_guard<std::mutex> lock(election_mutex_);
            // Execute Lua renewal script
            RedisReplyPtr reply((redisReply*)redisCommand(
                election_ctx_, "EVAL %s 1 %b %d %b", renewal_script,
                master_view_key_.data(), master_view_key_.size(), ttl_sec_,
                our_value_.data(), our_value_.size()));

            if (!reply) {
                LOG(ERROR)
                    << "KeepLeader: Lua renewal failed (connection error)";
                if (Reconnect(election_ctx_)) {
                    // Re-run the ownership check and renewal atomically after
                    // reconnect. GET followed by EXPIRE would allow another
                    // node to replace the key between the two commands.
                    RedisReplyPtr retry((redisReply*)redisCommand(
                        election_ctx_, "EVAL %s 1 %b %d %b", renewal_script,
                        master_view_key_.data(), master_view_key_.size(),
                        ttl_sec_, our_value_.data(), our_value_.size()));
                    if (retry && retry->type == REDIS_REPLY_INTEGER &&
                        retry->integer == 1) {
                        renewed = true;
                    } else {
                        LOG(WARNING) << "KeepLeader: Lua renewal failed after "
                                        "reconnect, key may have changed";
                    }
                }
            } else if (reply->type == REDIS_REPLY_INTEGER &&
                       reply->integer == 1) {
                // Renewal succeeded
                renewed = true;
            } else {
                // Key no longer ours
                LOG(WARNING)
                    << "KeepLeader: lost leadership (key no longer ours)";
            }
        }

        if (!renewed) {
            P2PHAMetricManager::instance().inc_election_leadership_lost();
            break;  // Lost leadership
        }

        WaitForKeepAliveCancellation(
            std::chrono::seconds(heartbeat_interval_sec_));
    }

    keep_alive_running_ = false;
    P2PHAMetricManager::instance().set_election_is_leader(false);
    LOG(INFO) << "KeepLeader: exited renewal loop";
}

void RedisElectionHelper::CancelKeepAlive() {
    cancel_keep_alive_ = true;
    keep_alive_running_ = false;
    cancel_cv_.notify_all();
}

void RedisElectionHelper::CancelElection() {
    cancel_election_ = true;
    cancel_cv_.notify_all();
}

void RedisElectionHelper::Shutdown() {
    {
        std::lock_guard<std::mutex> lock(operation_mutex_);
        shutting_down_ = true;
    }
    CancelElection();
    CancelKeepAlive();

    std::unique_lock<std::mutex> lock(operation_mutex_);
    operation_cv_.wait(lock,
                       [this] { return active_blocking_operations_ == 0; });
}

// ============================================================
// GetMasterView
// ============================================================

ErrorCode RedisElectionHelper::GetMasterView(std::string& master_address,
                                             ViewVersionId& version) {
    std::lock_guard<std::mutex> lock(election_mutex_);
    if (!election_ctx_) {
        LOG(ERROR) << "GetMasterView: not connected to Redis at "
                   << redis_endpoint_;
        return ErrorCode::INTERNAL_ERROR;
    }

    RedisReplyPtr reply((redisReply*)redisCommand(election_ctx_, "GET %b",
                                                  master_view_key_.data(),
                                                  master_view_key_.size()));

    if (!reply) {
        LOG(ERROR) << "GetMasterView: GET failed (connection error) at "
                   << redis_endpoint_;
        return ErrorCode::INTERNAL_ERROR;
    }

    if (reply->type == REDIS_REPLY_NIL) {
        LOG(WARNING) << "GetMasterView: no leader currently elected";
        return ErrorCode::INTERNAL_ERROR;
    }

    if (reply->type == REDIS_REPLY_STRING) {
        std::string value(reply->str, reply->len);
        if (ParseLeaderValue(value, master_address, version)) {
            return ErrorCode::OK;
        }
        LOG(ERROR) << "GetMasterView: failed to parse leader value: " << value;
        return ErrorCode::INTERNAL_ERROR;
    }

    int reply_type = reply->type;
    LOG(ERROR) << "GetMasterView: unexpected reply type=" << reply_type;
    return ErrorCode::INTERNAL_ERROR;
}

// ============================================================
// Internal helpers
// ============================================================

void RedisElectionHelper::PublishLeaderEvent(const std::string& master_address,
                                             ViewVersionId epoch) {
    // TBase does not broadcast PUBLISH invoked inside EVAL through its proxy,
    // so publish from the client after the atomic election script succeeds.
    const std::string event =
        "elected:" + master_address + ":" + std::to_string(epoch);
    RedisReplyPtr reply((redisReply*)redisCommand(
        election_ctx_, "PUBLISH %b %b", leader_event_channel_.data(),
        leader_event_channel_.size(), event.data(), event.size()));
    if (!reply) {
        LOG(WARNING) << "PublishLeaderEvent: PUBLISH failed; polling will "
                        "detect the leader change";
    }
}

bool RedisElectionHelper::Reconnect(redisContext*& ctx) {
    // Caller must hold election_mutex_ if reconnecting election_ctx_
    if (ctx) {
        redisFree(ctx);
        ctx = nullptr;
    }

    ctx = CreateConnection();
    if (!ctx) {
        LOG(ERROR) << "Reconnect: failed to connect to Redis";
        return false;
    }

    LOG(INFO) << "Reconnect: successfully reconnected to Redis";
    P2PHAMetricManager::instance().inc_election_reconnects();
    return true;
}

bool RedisElectionHelper::ReconnectSubscribeLocked(bool record_metric) {
    if (subscribe_ctx_) {
        redisFree(subscribe_ctx_);
        subscribe_ctx_ = nullptr;
    }
    subscribe_ctx_ = CreateConnection();
    if (!subscribe_ctx_) {
        LOG(ERROR) << "ReconnectSubscribe: failed to connect to Redis";
        return false;
    }
    LOG(INFO) << "ReconnectSubscribe: subscribe connection ready";
    if (record_metric) {
        P2PHAMetricManager::instance().inc_election_reconnects();
    }
    return true;
}

bool RedisElectionHelper::BeginBlockingOperation() {
    std::lock_guard<std::mutex> lock(operation_mutex_);
    if (shutting_down_) {
        return false;
    }
    ++active_blocking_operations_;
    return true;
}

bool RedisElectionHelper::BeginKeepAliveOperation() {
    std::lock_guard<std::mutex> lock(operation_mutex_);
    if (shutting_down_ || keep_alive_operation_active_) {
        return false;
    }
    cancel_keep_alive_ = false;
    keep_alive_operation_active_ = true;
    ++active_blocking_operations_;
    return true;
}

void RedisElectionHelper::EndBlockingOperation() {
    {
        std::lock_guard<std::mutex> lock(operation_mutex_);
        --active_blocking_operations_;
    }
    operation_cv_.notify_all();
}

void RedisElectionHelper::EndKeepAliveOperation() {
    {
        std::lock_guard<std::mutex> lock(operation_mutex_);
        keep_alive_operation_active_ = false;
        --active_blocking_operations_;
    }
    operation_cv_.notify_all();
}

bool RedisElectionHelper::WaitForElectionCancellation(
    std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(cancel_mutex_);
    return cancel_cv_.wait_for(lock, timeout,
                               [this] { return cancel_election_.load(); });
}

bool RedisElectionHelper::WaitForKeepAliveCancellation(
    std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(cancel_mutex_);
    return cancel_cv_.wait_for(lock, timeout,
                               [this] { return cancel_keep_alive_.load(); });
}

// ============================================================
// Serialization
// ============================================================

std::string RedisElectionHelper::SerializeLeaderValue(
    const std::string& address, ViewVersionId epoch, int ttl_sec) {
    Json::Value root;
    root["address"] = address;
    root["epoch"] = static_cast<Json::Int64>(epoch);
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch())
                  .count();
    root["ts"] = static_cast<Json::Int64>(ms);
    root["ttl"] = ttl_sec;

    Json::StreamWriterBuilder builder;
    builder["commentStyle"] = "None";
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

bool RedisElectionHelper::ParseLeaderValue(const std::string& json,
                                           std::string& out_address,
                                           ViewVersionId& out_epoch) {
    return ParseElectionValue(json, out_address, out_epoch, nullptr);
}

}  // namespace mooncake

#endif  // STORE_USE_REDIS

#ifdef STORE_USE_REDIS

#include "p2p/ha/redis_election_helper.h"

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
#include <utility>

#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/ha_status.h"

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

std::string GenerateInstanceId() {
    std::random_device random;
    std::mt19937_64 generator(random());
    std::ostringstream stream;
    stream << std::hex << std::setfill('0') << std::setw(16) << generator()
           << std::setw(16) << generator();
    return stream.str();
}

int64_t CurrentTimestampMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool ParseElectionValue(const std::string& value, std::string& address,
                        ViewVersionId& epoch, std::string* instance_id,
                        int64_t* timestamp_ms = nullptr,
                        int64_t* ttl_sec = nullptr) {
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
    if (instance_id) {
        instance_id->clear();
        if (root.isMember("instance_id")) {
            if (!root["instance_id"].isString()) {
                return false;
            }
            *instance_id = root["instance_id"].asString();
        }
    }
    if (timestamp_ms) {
        *timestamp_ms = 0;
        if (root.isMember("ts")) {
            if (!root["ts"].isIntegral()) return false;
            *timestamp_ms = root["ts"].asInt64();
        }
    }
    if (ttl_sec) {
        *ttl_sec = 0;
        if (root.isMember("ttl")) {
            if (!root["ttl"].isIntegral()) return false;
            *ttl_sec = root["ttl"].asInt64();
        }
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
                                         const std::string& username,
                                         const std::string& instance_id)
    : redis_endpoint_(redis_endpoint),
      username_(username),
      password_(password),
      db_index_(db_index),
      ttl_sec_(ttl_sec),
      heartbeat_interval_sec_(heartbeat_interval_sec),
      instance_id_(instance_id.empty() ? GenerateInstanceId() : instance_id),
      cluster_id_(cluster_id) {
    const std::string& cid = cluster_id_;
    // Use {cid} with braces as hash tag for Redis cluster slot affinity
    master_view_key_ = "mooncake:{" + cid + "}:master_view";
    master_epoch_key_ = "mooncake:{" + cid + "}:master_epoch";
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

void RedisElectionHelper::SetShouldElect(ShouldElect should_elect) {
    should_elect_ = std::move(should_elect);
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
        if (should_elect_ && !should_elect_()) {
            HAStatusManager::instance().SetDoctorIssue(
                "ELECTION_NOT_READY",
                "ShouldElect rejected this leader attempt");
            WaitForElectionCancellation(std::chrono::milliseconds(200));
            continue;
        }
        HAStatusManager::instance().SetDoctorIssue("ELECTION_NOT_READY");

        HAMetricManager::instance().inc_election_attempts();
        bool connected = false;
        {
            std::lock_guard<std::mutex> lock(election_mutex_);
            if (!election_ctx_) {
                election_ctx_ = CreateConnection();
                if (!election_ctx_) {
                    HAMetricManager::instance().inc_election_failures();
                    LOG(ERROR) << "ElectLeader: connect failed, retry in 1s";
                }
            }
            connected = (election_ctx_ != nullptr);
        }
        if (!connected) {
            HAStatusManager::instance().SetDoctorIssue(
                "ELECTION_FAILED", "Cannot connect to Redis for election");
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
            HAStatusManager::instance().SetDoctorIssue(
                "ELECTION_FAILED", "Redis election attempt failed; retrying");
            WaitForElectionCancellation(std::chrono::seconds(1));
            continue;
        }
        HAStatusManager::instance().SetDoctorIssue("ELECTION_FAILED");
        if (attempt == ElectionAttemptResult::ELECTED) {
            HAStatusManager::instance().SetDoctorIssue("LEADERSHIP_LOST");
            {
                std::lock_guard<std::mutex> lock(election_mutex_);
                PublishLeaderEvent("elected:" + master_address + ":" +
                                   std::to_string(version));
            }
            HAMetricManager::instance().set_election_is_leader(true);
            HAMetricManager::instance().observe_election_duration_ms(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - election_start)
                    .count());
            lease_id = next_lease_id_++;
            LOG(INFO) << "ElectLeader: elected as leader, epoch=" << version
                      << " lease_id=" << lease_id;
            return;
        }

        std::string current_addr;
        std::string current_instance_id;
        int64_t current_timestamp_ms = 0;
        int64_t current_ttl_sec = 0;
        ViewVersionId current_epoch = 0;
        if (ParseElectionValue(existing_value, current_addr, current_epoch,
                               &current_instance_id, &current_timestamp_ms,
                               &current_ttl_sec)) {
            HAStatusManager::instance().SetMasterView(
                current_addr, current_epoch, current_timestamp_ms,
                current_ttl_sec, current_instance_id);
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
        "  instance_id=ARGV[4]}) "
        "redis.call('SET', KEYS[1], value, 'EX', ARGV[3]) "
        "return {1, tostring(epoch), value}";

    const std::string& instance_id = instance_id_;
    const auto timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    RedisReplyPtr reply((redisReply*)redisCommand(
        election_ctx_, "EVAL %s 2 %b %b %b %lld %d %b", election_script,
        master_view_key_.data(), master_view_key_.size(),
        master_epoch_key_.data(), master_epoch_key_.size(),
        master_address.data(), master_address.size(),
        static_cast<long long>(timestamp_ms), ttl_sec_, instance_id.data(),
        instance_id.size()));

    if (!reply) {
        HAMetricManager::instance().inc_election_failures();

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
                   "reconnect"
                << ", error="
                << RedisUtil::FormatCommandError(election_ctx_, current.get());
            return ElectionAttemptResult::ERROR;
        }
        std::string value(current->str, current->len);
        std::string address;
        std::string stored_instance_id;
        int64_t stored_timestamp_ms = 0;
        int64_t stored_ttl_sec = 0;
        ViewVersionId epoch = 0;
        if (ParseElectionValue(value, address, epoch, &stored_instance_id,
                               &stored_timestamp_ms, &stored_ttl_sec) &&
            stored_instance_id == instance_id) {
            out_epoch = epoch;
            our_value_ = std::move(value);
            HAStatusManager::instance().SetMasterView(
                address, epoch, stored_timestamp_ms, stored_ttl_sec,
                stored_instance_id);
            LOG(WARNING) << "TryElectOnce: election script response was lost; "
                            "confirmed election after reconnect";
            return ElectionAttemptResult::ELECTED;
        }
        LOG(WARNING) << "TryElectOnce: election result after reconnect does "
                        "not match this attempt";
        return ElectionAttemptResult::ERROR;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        HAMetricManager::instance().inc_election_failures();
        LOG(WARNING) << "TryElectOnce: election script failed, error="
                     << RedisUtil::FormatCommandError(election_ctx_,
                                                      reply.get());
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
        HAMetricManager::instance().inc_election_failures();
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
    int64_t written_timestamp_ms = 0;
    int64_t written_ttl_sec = 0;
    std::string address;
    ViewVersionId parsed_epoch = 0;
    if (ParseElectionValue(our_value_, address, parsed_epoch, nullptr,
                           &written_timestamp_ms, &written_ttl_sec)) {
        HAStatusManager::instance().SetMasterView(master_address, parsed_epoch,
                                                  written_timestamp_ms,
                                                  written_ttl_sec, instance_id);
    }
    return ElectionAttemptResult::ELECTED;
}

// ============================================================
// WatchLeader — wait until leader key expires
// ============================================================

void RedisElectionHelper::WatchLeader() {
    ScopeGuard watch_status([] {
        HAStatusManager::instance().SetDoctorIssue("ELECTION_WATCH_FAILED");
    });
    // Fast path: SUBSCRIBE for leader event notification
    if (WatchLeaderSubscribe()) {
        return;
    }

    // Slow path (fallback): pure polling — use a separate connection
    HAMetricManager::instance().inc_election_watch_failures();
    HAMetricManager::instance().inc_election_polling_fallbacks();
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
    // Only this thread accesses the connection until it has been joined.
    std::thread polling_thread([this, &leader_lost, &polling_ctx]() {
        auto interval = std::chrono::seconds(ttl_sec_);
        while (!leader_lost && !cancel_election_) {
            if (PollLeaderOnce(polling_ctx)) {
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
        if (PollLeaderOnce(polling_ctx)) {
            break;  // Key expired.
        }

        if (WaitForElectionCancellation(interval)) {
            break;
        }
    }
    if (polling_ctx) redisFree(polling_ctx);
}

bool RedisElectionHelper::PollLeaderOnce(redisContext*& context) {
    if (!context) {
        context = CreateConnection();
        if (!context) {
            // CreateConnection logs the underlying connection/auth failure.
            HAStatusManager::instance().SetDoctorIssue(
                "ELECTION_WATCH_FAILED",
                "Cannot connect to Redis while watching leader");
            return false;
        }
    }

    RedisReplyPtr reply((redisReply*)redisCommand(
        context, "GET %b", master_view_key_.data(), master_view_key_.size()));
    if (!reply ||
        (reply->type != REDIS_REPLY_STRING && reply->type != REDIS_REPLY_NIL)) {
        const auto error = RedisUtil::FormatCommandError(context, reply.get());
        LOG_EVERY_N(WARNING, 30)
            << "WatchLeader: polling GET failed; reconnecting on next poll"
            << ", key=" << master_view_key_ << ", error=" << error;
        HAStatusManager::instance().SetDoctorIssue(
            "ELECTION_WATCH_FAILED", "Leader polling GET failed: " + error);
        redisFree(context);
        context = nullptr;
        return false;
    }
    HAStatusManager::instance().SetDoctorIssue("ELECTION_WATCH_FAILED");
    return reply->type == REDIS_REPLY_NIL;
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
                LOG(ERROR) << "KeepLeader: Lua renewal failed"
                           << ", error="
                           << RedisUtil::FormatCommandError(election_ctx_,
                                                            reply.get());
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
                                        "reconnect, "
                                        "key may have changed"
                                     << ", error="
                                     << RedisUtil::FormatCommandError(
                                            election_ctx_, retry.get());
                    }
                }
            } else if (reply->type == REDIS_REPLY_INTEGER &&
                       reply->integer == 1) {
                // Renewal succeeded
                renewed = true;
            } else if (reply->type == REDIS_REPLY_ERROR) {
                LOG(ERROR) << "KeepLeader: Lua renewal failed"
                           << ", error="
                           << RedisUtil::FormatCommandError(election_ctx_,
                                                            reply.get());
            } else {
                // Key no longer ours
                LOG(WARNING) << "KeepLeader: lost leadership (key no longer "
                                "ours)"
                             << ", reply_type=" << reply->type;
            }
        }

        if (!renewed) {
            HAStatusManager::instance().SetDoctorIssue(
                "LEADERSHIP_LOST",
                "Leadership renewal failed or lease ownership changed");
            HAMetricManager::instance().inc_election_renew_failures();
            HAMetricManager::instance().inc_election_leadership_lost();
            break;  // Lost leadership
        }

        HAMetricManager::instance()
            .set_election_last_successful_renew_timestamp_ms(
                CurrentTimestampMs());
        HAStatusManager::instance().SetDoctorIssue("LEADERSHIP_LOST");
        // Refresh the reported remaining lease so /ha/status does not drift
        // negative under ongoing renewals.
        HAStatusManager::instance().MarkMasterViewRefreshed();
        WaitForKeepAliveCancellation(
            std::chrono::seconds(heartbeat_interval_sec_));
    }

    keep_alive_running_ = false;
    HAMetricManager::instance().set_election_is_leader(false);
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

ErrorCode RedisElectionHelper::ReleaseLeader() {
    std::lock_guard<std::mutex> lock(election_mutex_);
    if (our_value_.empty()) {
        return ErrorCode::OK;
    }
    if ((!election_ctx_ || election_ctx_->err) && !Reconnect(election_ctx_)) {
        return ErrorCode::INTERNAL_ERROR;
    }

    static constexpr const char* kReleaseScript =
        "if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end; "
        "return redis.call('DEL', KEYS[1])";
    RedisReplyPtr reply((redisReply*)redisCommand(
        election_ctx_, "EVAL %s 1 %b %b", kReleaseScript,
        master_view_key_.data(), master_view_key_.size(), our_value_.data(),
        our_value_.size()));
    if (!reply || reply->type != REDIS_REPLY_INTEGER ||
        (reply->integer != 0 && reply->integer != 1)) {
        LOG(WARNING) << "ReleaseLeader: failed to release Redis leadership"
                     << ", error="
                     << RedisUtil::FormatCommandError(election_ctx_,
                                                      reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }

    LOG(INFO) << "ReleaseLeader: Redis leadership release completed"
              << ", released=" << reply->integer;
    if (reply->integer == 1) {
        PublishLeaderEvent("released");
    }
    our_value_.clear();
    return ErrorCode::OK;
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

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "GetMasterView: GET failed at " << redis_endpoint_
                   << ", error="
                   << RedisUtil::FormatCommandError(election_ctx_, reply.get());
        return ErrorCode::INTERNAL_ERROR;
    }

    if (reply->type == REDIS_REPLY_NIL) {
        LOG(WARNING) << "GetMasterView: no leader currently elected";
        return ErrorCode::INTERNAL_ERROR;
    }

    if (reply->type == REDIS_REPLY_STRING) {
        std::string value(reply->str, reply->len);
        if (ParseLeaderValue(value, master_address, version)) {
            std::string instance_id;
            int64_t timestamp_ms = 0;
            int64_t ttl_sec = 0;
            if (ParseElectionValue(value, master_address, version, &instance_id,
                                   &timestamp_ms, &ttl_sec)) {
                HAStatusManager::instance().SetMasterView(master_address,
                                                          version, timestamp_ms,
                                                          ttl_sec, instance_id);
            }
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

void RedisElectionHelper::PublishLeaderEvent(const std::string& event) {
    // TBase does not broadcast PUBLISH invoked inside EVAL through its proxy,
    // so publish from the client after an atomic ownership update succeeds.
    RedisReplyPtr reply((redisReply*)redisCommand(
        election_ctx_, "PUBLISH %b %b", leader_event_channel_.data(),
        leader_event_channel_.size(), event.data(), event.size()));
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        LOG(WARNING) << "PublishLeaderEvent: PUBLISH failed; polling will "
                        "detect the leader change"
                     << ", error="
                     << RedisUtil::FormatCommandError(election_ctx_,
                                                      reply.get());
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
    HAMetricManager::instance().inc_election_reconnects();
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
        HAMetricManager::instance().inc_election_reconnects();
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
    if (shutting_down_ || cancel_keep_alive_ || keep_alive_operation_active_) {
        return false;
    }
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

#ifdef STORE_USE_REDIS

#include "redis_helper.h"

#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <json/json.h>

namespace mooncake {

// ============================================================
// Construction / Destruction
// ============================================================

RedisHelper::RedisHelper(const std::string& cluster_id,
                         const std::string& redis_endpoint,
                         const std::string& password, int db_index, int ttl_sec,
                         int heartbeat_interval_sec)
    : redis_endpoint_(redis_endpoint),
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
    LOG(INFO) << "RedisHelper created, master_view_key=" << master_view_key_
              << " epoch_key=" << master_epoch_key_
              << " channel=" << leader_event_channel_ << " ttl=" << ttl_sec_
              << "s"
              << " heartbeat=" << heartbeat_interval_sec_ << "s";
}

RedisHelper::~RedisHelper() {
    CancelKeepAlive();
    if (subscribe_ctx_) {
        redisFree(subscribe_ctx_);
        subscribe_ctx_ = nullptr;
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

redisContext* RedisHelper::CreateConnection() {
    std::string host = "127.0.0.1";
    int port = 6379;
    auto colon_pos = redis_endpoint_.rfind(':');
    if (colon_pos != std::string::npos) {
        host = redis_endpoint_.substr(0, colon_pos);
        try {
            port = std::stoi(redis_endpoint_.substr(colon_pos + 1));
        } catch (const std::exception& e) {
            LOG(ERROR) << "Invalid Redis endpoint port: " << redis_endpoint_;
            return nullptr;
        }
    } else if (!redis_endpoint_.empty()) {
        host = redis_endpoint_;
    }

    struct timeval tv;
    tv.tv_sec = connect_timeout_ms_ / 1000;
    tv.tv_usec = (connect_timeout_ms_ % 1000) * 1000;

    redisContext* ctx = redisConnectWithTimeout(host.c_str(), port, tv);
    if (!ctx || ctx->err) {
        LOG(ERROR) << "Failed to connect to Redis at " << host << ":" << port
                   << " err=" << (ctx ? ctx->errstr : "null");
        if (ctx) {
            redisFree(ctx);
        }
        return nullptr;
    }
    redisSetTimeout(ctx, tv);

    // Authenticate if password provided
    if (!password_.empty()) {
        redisReply* reply = (redisReply*)redisCommand(
            ctx, "AUTH %b", password_.data(), password_.size());
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "Redis AUTH failed";
            if (reply) freeReplyObject(reply);
            redisFree(ctx);
            return nullptr;
        }
        freeReplyObject(reply);
    }

    // Select DB if not default
    if (db_index_ != 0) {
        redisReply* reply =
            (redisReply*)redisCommand(ctx, "SELECT %d", db_index_);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LOG(ERROR) << "Redis SELECT " << db_index_ << " failed";
            if (reply) freeReplyObject(reply);
            redisFree(ctx);
            return nullptr;
        }
        freeReplyObject(reply);
    }

    return ctx;
}

// ============================================================
// Connect
// ============================================================

ErrorCode RedisHelper::Connect() {
    // Election connection
    {
        std::lock_guard<std::mutex> lock(election_mutex_);
        if (election_ctx_) {
            redisFree(election_ctx_);
            election_ctx_ = nullptr;
        }
        election_ctx_ = CreateConnection();
        if (!election_ctx_) {
            return ErrorCode::INTERNAL_ERROR;
        }
    }

    // Subscribe connection (separate, as SUBSCRIBE blocks).
    // Set a 1-second read timeout so that redisGetReply in WatchLeader's
    // subscribe loop returns periodically, allowing the loop to check
    // notified / cancel_requested_ flags even when no Pub/Sub message
    // arrives (e.g. leader key expired without graceful handoff).
    if (subscribe_ctx_) {
        redisFree(subscribe_ctx_);
        subscribe_ctx_ = nullptr;
    }
    subscribe_ctx_ = CreateConnection();
    if (subscribe_ctx_) {
        struct timeval sub_timeout = {1, 0};  // 1 second
        redisSetTimeout(subscribe_ctx_, sub_timeout);
    } else {
        // Non-fatal: WatchLeader will fall back to polling
        LOG(ERROR) << "Failed to create subscribe connection to Redis";
    }

    LOG(INFO) << "Connected to Redis";
    return ErrorCode::OK;
}

// ============================================================
// ElectLeader — blocks until this node wins election
// ============================================================

void RedisHelper::ElectLeader(const std::string& master_address,
                              ViewVersionId& version, int& lease_id) {
    while (true) {
        {
            std::lock_guard<std::mutex> lock(election_mutex_);
            if (!election_ctx_) {
                election_ctx_ = CreateConnection();
                if (!election_ctx_) {
                    LOG(ERROR) << "ElectLeader: connect failed, retry in 1s";
                }
            }
        }
        if (!election_ctx_) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        // Step 1: Check if a leader already exists
        redisReply* reply = nullptr;
        {
            std::lock_guard<std::mutex> lock(election_mutex_);
            reply = (redisReply*)redisCommand(election_ctx_, "GET %b",
                                              master_view_key_.data(),
                                              master_view_key_.size());
        }

        if (!reply) {
            LOG(ERROR)
                << "ElectLeader: GET failed (connection error), retry in 1s";
            {
                std::lock_guard<std::mutex> lock(election_mutex_);
                Reconnect(election_ctx_);
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        if (reply->type == REDIS_REPLY_NIL) {
            // No leader exists — try to elect ourselves
            freeReplyObject(reply);

            bool elected = false;
            {
                std::lock_guard<std::mutex> lock(election_mutex_);
                elected = TryElectOnce(master_address, version);
            }
            if (elected) {
                lease_id = next_lease_id_++;
                LOG(INFO) << "ElectLeader: elected as leader, epoch=" << version
                          << " lease_id=" << lease_id;
                return;
            }
            // TryElectOnce failed (someone else won) — loop back and wait
            continue;
        }

        if (reply->type == REDIS_REPLY_STRING) {
            // A leader exists — watch until the key expires
            std::string current_value(reply->str, reply->len);
            freeReplyObject(reply);

            std::string current_addr;
            ViewVersionId current_epoch = 0;
            if (ParseLeaderValue(current_value, current_addr, current_epoch)) {
                LOG(INFO) << "ElectLeader: current leader=" << current_addr
                          << " epoch=" << current_epoch << ", waiting...";
            } else {
                LOG(INFO) << "ElectLeader: leader key exists but unparsable, "
                             "waiting...";
            }

            WatchLeader();  // Blocks until key expires or "vacant" received
            continue;
        }

        // Unexpected reply type
        freeReplyObject(reply);
        LOG(ERROR) << "ElectLeader: unexpected reply type=" << reply->type;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

bool RedisHelper::TryElectOnce(const std::string& master_address,
                               ViewVersionId& out_epoch) {
    // Caller must hold election_mutex_
    // Step 2: INCR epoch counter
    redisReply* reply = (redisReply*)redisCommand(election_ctx_, "INCR %b",
                                                  master_epoch_key_.data(),
                                                  master_epoch_key_.size());

    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        LOG(ERROR) << "TryElectOnce: INCR failed";
        if (reply) freeReplyObject(reply);
        return false;
    }
    out_epoch = reply->integer;
    freeReplyObject(reply);

    // Step 3: SET NX EX — atomically create leader key only if it doesn't exist
    std::string value =
        SerializeLeaderValue(master_address, out_epoch, ttl_sec_);
    our_value_ = value;

    reply = (redisReply*)redisCommand(
        election_ctx_, "SET %b %b EX %d NX", master_view_key_.data(),
        master_view_key_.size(), value.data(), value.size(), ttl_sec_);

    if (!reply) {
        LOG(ERROR) << "TryElectOnce: SET NX EX failed (connection error)";
        our_value_.clear();
        return false;
    }

    if (reply->type == REDIS_REPLY_STATUS &&
        strncmp(reply->str, "OK", 2) == 0) {
        // We won the election!
        freeReplyObject(reply);

        // Publish election event
        std::string event =
            "elected:" + master_address + ":" + std::to_string(out_epoch);
        PublishLeaderEvent(event);

        return true;
    }

    // Key already exists — someone else won
    freeReplyObject(reply);
    our_value_.clear();
    LOG(INFO) << "TryElectOnce: someone else won the election, retrying";
    return false;
}

// ============================================================
// WatchLeader — wait until leader key expires
// ============================================================

void RedisHelper::WatchLeader() {
    // Fast path: SUBSCRIBE for leader event notification
    if (subscribe_ctx_) {
        redisReply* reply = (redisReply*)redisCommand(
            subscribe_ctx_, "SUBSCRIBE %b", leader_event_channel_.data(),
            leader_event_channel_.size());

        if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3 &&
            reply->element[0]->type == REDIS_REPLY_STRING &&
            strncmp(reply->element[0]->str, "subscribe", 9) == 0) {
            freeReplyObject(reply);

            std::atomic<bool> notified{false};
            redisContext* polling_ctx = CreateConnection();

            // Polling thread: check if key still exists periodically.
            // The subscribe_ctx_ has a 1-second timeout set in Connect(),
            // so redisGetReply returns at least once per second regardless
            // of whether a Pub/Sub message arrives, allowing the subscribe
            // loop to check notified/cancel_requested_ flags.
            std::thread polling_thread([this, &notified, polling_ctx]() {
                auto interval = std::chrono::seconds(ttl_sec_);
                while (!notified && !cancel_requested_) {
                    std::this_thread::sleep_for(interval);
                    if (notified || cancel_requested_) break;

                    if (polling_ctx) {
                        redisReply* r = (redisReply*)redisCommand(
                            polling_ctx, "GET %b", master_view_key_.data(),
                            master_view_key_.size());
                        if (r) {
                            if (r->type == REDIS_REPLY_NIL) {
                                notified = true;
                            }
                            freeReplyObject(r);
                        }
                    }
                }
            });

            // Subscribe loop: read messages until "vacant" or key gone.
            // redisGetReply returns at least every 1s (subscribe_ctx_ timeout)
            // so the loop can check notified/cancel_requested_ promptly.
            while (!notified && !cancel_requested_) {
                redisReply* msg = nullptr;
                if (redisGetReply(subscribe_ctx_, (void**)&msg) != REDIS_OK) {
                    // Timeout or connection error — re-check flags and retry
                    continue;
                }

                if (msg) {
                    if (msg->type == REDIS_REPLY_ARRAY && msg->elements >= 3) {
                        if (msg->element[0]->type == REDIS_REPLY_STRING &&
                            strncmp(msg->element[0]->str, "message", 7) == 0) {
                            notified = true;
                        }
                    }
                    freeReplyObject(msg);
                }
            }

            notified = true;  // Signal polling thread to stop
            polling_thread.join();

            // Clean up polling connection
            if (polling_ctx) {
                redisFree(polling_ctx);
            }

            // Unsubscribe to restore connection state
            redisReply* unsub = (redisReply*)redisCommand(
                subscribe_ctx_, "UNSUBSCRIBE %b", leader_event_channel_.data(),
                leader_event_channel_.size());
            if (unsub) freeReplyObject(unsub);

            return;
        }
        if (reply) freeReplyObject(reply);
        // Subscribe failed — fall through to pure polling
    }

    // Slow path (fallback): pure polling — use a separate connection
    LOG(INFO) << "WatchLeader: using polling fallback (interval=" << ttl_sec_
              << "s)";
    redisContext* polling_ctx = CreateConnection();
    auto interval = std::chrono::seconds(ttl_sec_);
    while (!cancel_requested_) {
        std::this_thread::sleep_for(interval);

        if (!polling_ctx) {
            // Connection was never established or previously lost — retry
            polling_ctx = CreateConnection();
            continue;
        }

        redisReply* reply = (redisReply*)redisCommand(polling_ctx, "GET %b",
                                                      master_view_key_.data(),
                                                      master_view_key_.size());
        if (reply) {
            if (reply->type == REDIS_REPLY_NIL) {
                freeReplyObject(reply);
                redisFree(polling_ctx);
                return;  // Key expired
            }
            freeReplyObject(reply);
        } else {
            // Connection error — reconnect
            redisFree(polling_ctx);
            polling_ctx = nullptr;  // Will retry CreateConnection next loop
        }
    }
    if (polling_ctx) redisFree(polling_ctx);
}

// ============================================================
// KeepLeader — renew TTL via Lua script, block until lost
// ============================================================

void RedisHelper::KeepLeader(int lease_id) {
    keep_alive_running_ = true;
    cancel_requested_ = false;

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

    while (keep_alive_running_ && !cancel_requested_) {
        bool renewed = false;
        {
            std::lock_guard<std::mutex> lock(election_mutex_);
            // Execute Lua renewal script
            redisReply* reply = (redisReply*)redisCommand(
                election_ctx_, "EVAL %s 1 %b %d %b", renewal_script,
                master_view_key_.data(), master_view_key_.size(), ttl_sec_,
                our_value_.data(), our_value_.size());

            if (!reply) {
                LOG(ERROR)
                    << "KeepLeader: Lua renewal failed (connection error)";
                if (Reconnect(election_ctx_)) {
                    // Reconnected — check if we still own the key
                    redisReply* check = (redisReply*)redisCommand(
                        election_ctx_, "GET %b", master_view_key_.data(),
                        master_view_key_.size());
                    if (check && check->type == REDIS_REPLY_STRING &&
                        check->len == our_value_.size() &&
                        memcmp(check->str, our_value_.data(),
                               our_value_.size()) == 0) {
                        // Still ours — continue renewing
                        freeReplyObject(check);
                        renewed = true;
                    } else {
                        // Not ours anymore
                        if (check) freeReplyObject(check);
                    }
                }
            } else if (reply->type == REDIS_REPLY_INTEGER &&
                       reply->integer == 1) {
                // Renewal succeeded
                freeReplyObject(reply);
                renewed = true;
            } else {
                // Key no longer ours
                freeReplyObject(reply);
                LOG(WARNING)
                    << "KeepLeader: lost leadership (key no longer ours)";
            }
        }

        if (!renewed) {
            break;  // Lost leadership
        }

        std::this_thread::sleep_for(
            std::chrono::seconds(heartbeat_interval_sec_));
    }

    keep_alive_running_ = false;
    LOG(INFO) << "KeepLeader: exited renewal loop";
}

void RedisHelper::CancelKeepAlive() {
    cancel_requested_ = true;
    keep_alive_running_ = false;
    // subscribe_ctx_ has a 1-second timeout, so WatchLeader's subscribe
    // loop will check cancel_requested_ within 1 second.
}

// ============================================================
// GetMasterView
// ============================================================

ErrorCode RedisHelper::GetMasterView(std::string& master_address,
                                     ViewVersionId& version) {
    std::lock_guard<std::mutex> lock(election_mutex_);
    if (!election_ctx_) {
        return ErrorCode::INTERNAL_ERROR;
    }

    redisReply* reply = (redisReply*)redisCommand(election_ctx_, "GET %b",
                                                  master_view_key_.data(),
                                                  master_view_key_.size());

    if (!reply) {
        return ErrorCode::INTERNAL_ERROR;
    }

    if (reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(reply);
        return ErrorCode::INTERNAL_ERROR;  // No leader
    }

    if (reply->type == REDIS_REPLY_STRING) {
        std::string value(reply->str, reply->len);
        freeReplyObject(reply);
        if (ParseLeaderValue(value, master_address, version)) {
            return ErrorCode::OK;
        }
        return ErrorCode::INTERNAL_ERROR;
    }

    freeReplyObject(reply);
    return ErrorCode::INTERNAL_ERROR;
}

// ============================================================
// Internal helpers
// ============================================================

void RedisHelper::PublishLeaderEvent(const std::string& event) {
    // Caller must hold election_mutex_
    if (!election_ctx_) return;
    redisReply* reply = (redisReply*)redisCommand(
        election_ctx_, "PUBLISH %b %b", leader_event_channel_.data(),
        leader_event_channel_.size(), event.data(), event.size());
    if (reply) freeReplyObject(reply);
}

bool RedisHelper::Reconnect(redisContext*& ctx) {
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
    return true;
}

// ============================================================
// Serialization
// ============================================================

std::string RedisHelper::SerializeLeaderValue(const std::string& address,
                                              ViewVersionId epoch,
                                              int ttl_sec) {
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

bool RedisHelper::ParseLeaderValue(const std::string& json,
                                   std::string& out_address,
                                   ViewVersionId& out_epoch) {
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::istringstream stream(json);
    std::string errors;
    if (!Json::parseFromStream(builder, stream, &root, &errors)) {
        LOG(ERROR) << "ParseLeaderValue: JSON parse failed: " << errors;
        return false;
    }

    if (!root.isMember("address") || !root["address"].isString() ||
        !root.isMember("epoch") || !root["epoch"].isInt64()) {
        return false;
    }

    out_address = root["address"].asString();
    out_epoch = root["epoch"].asInt64();
    return true;
}

}  // namespace mooncake

#endif  // STORE_USE_REDIS
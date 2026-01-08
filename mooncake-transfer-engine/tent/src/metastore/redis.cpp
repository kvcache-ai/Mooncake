// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/metastore/redis.h"

#include <glog/logging.h>
#include <memory>

namespace mooncake {
namespace tent {

// RAII wrapper for redisReply
class RedisReplyGuard {
   public:
    explicit RedisReplyGuard(redisReply *reply) : reply_(reply) {}
    ~RedisReplyGuard() {
        if (reply_) freeReplyObject(reply_);
    }

    redisReply *get() const { return reply_; }
    redisReply *operator->() const { return reply_; }
    operator bool() const { return reply_ != nullptr; }

    // Non-copyable
    RedisReplyGuard(const RedisReplyGuard &) = delete;
    RedisReplyGuard &operator=(const RedisReplyGuard &) = delete;

   private:
    redisReply *reply_;
};

RedisMetaStore::RedisMetaStore() {}

RedisMetaStore::~RedisMetaStore() { disconnect(); }

Status RedisMetaStore::connect(const std::string &endpoint) {
    return connect(endpoint, "", 0);
}

Status RedisMetaStore::connect(const std::string &endpoint,
                               const std::string &password, uint8_t db_index) {
    if (connected_) {
        return Status::MetadataError(
            "Redis connection already established" LOC_MARK);
    }

    // Input validation
    if (endpoint.empty()) {
        return Status::MetadataError(
            std::string("Redis endpoint cannot be empty") + LOC_MARK);
    }

    // Remove password length restriction - let Redis server handle validation
    // Password validation is now handled by Redis server itself

    if (db_index > REDIS_MAX_DB_INDEX) {
        return Status::MetadataError(
            std::string("Redis database index out of range (0-255)") +
            LOC_MARK);
    }

    auto hostname_port = parseHostNameWithPort(endpoint, REDIS_DEFAULT_PORT);
    client_ = redisConnect(hostname_port.first.c_str(), hostname_port.second);
    if (!client_) {
        std::string message = "Redis cannot connect to endpoint";
        return Status::MetadataError(message + LOC_MARK);
    }
    if (client_->err) {
        std::string message =
            "Redis connection failed: " + std::string(client_->errstr);
        cleanupFailedConnection();
        return Status::MetadataError(message + LOC_MARK);
    }

    // Authenticate if password is provided
    if (!password.empty()) {
        // Use binary-safe authentication to prevent password leakage
        redisReply *reply = (redisReply *)redisCommand(
            client_, "AUTH %b", password.data(), password.size());
        RedisReplyGuard reply_guard(reply);

        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            // Don't leak specific error details that might contain sensitive
            // info
            std::string message = "Redis authentication failed";
            cleanupFailedConnection();
            return Status::MetadataError(message + LOC_MARK);
        }
    }

    // Select database if db_index is not 0
    if (db_index != REDIS_DEFAULT_DB_INDEX) {
        redisReply *reply =
            (redisReply *)redisCommand(client_, "SELECT %d", db_index);
        RedisReplyGuard reply_guard(reply);

        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            std::string message =
                "Redis failed to select database " + std::to_string(db_index);
            cleanupFailedConnection();
            return Status::MetadataError(message + LOC_MARK);
        }
    }

    connected_ = true;
    return Status::OK();
}

Status RedisMetaStore::disconnect() {
    if (connected_) {
        redisFree(client_);
        connected_ = false;
    }
    return Status::OK();
}

void RedisMetaStore::cleanupFailedConnection() {
    if (client_) {
        redisFree(client_);
        client_ = nullptr;
    }
    connected_ = false;
}

Status RedisMetaStore::handleRedisReply(redisReply *reply,
                                        const std::string &operation) const {
    if (!reply) {
        return Status::MetadataError("Redis " + operation +
                                     " failed: connection error" + LOC_MARK);
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        // Filter out potentially sensitive information from Redis error
        // messages
        std::string error_msg = reply->str ? reply->str : "unknown error";

        // Generic error message to avoid leaking sensitive information
        std::string message = "Redis " + operation + " failed: operation error";

        // Log detailed error for debugging (ensure logs are properly secured)
        LOG(WARNING) << "Redis operation failed - " << operation << ": "
                     << error_msg;

        return Status::MetadataError(message + LOC_MARK);
    }

    return Status::OK();
}

Status RedisMetaStore::get(const std::string &key, std::string &value) {
    if (!connected_) {
        return Status::MetadataError("Redis connection not available" LOC_MARK);
    }

    // Use binary-safe formatting to prevent command injection
    redisReply *resp =
        (redisReply *)redisCommand(client_, "GET %b", key.data(), key.size());
    RedisReplyGuard reply_guard(resp);

    Status status = handleRedisReply(resp, "GET '" + key + "'");
    if (!status.ok()) {
        return status;
    }

    if (resp->type == REDIS_REPLY_NIL) {
        return Status::InvalidEntry(key);
    }

    value = std::string(resp->str);
    return Status::OK();
}

Status RedisMetaStore::set(const std::string &key, const std::string &value) {
    if (!connected_) {
        return Status::MetadataError("Redis connection not available" LOC_MARK);
    }

    // Use binary-safe formatting to prevent command injection
    redisReply *resp =
        (redisReply *)redisCommand(client_, "SET %b %b", key.data(), key.size(),
                                   value.data(), value.size());
    RedisReplyGuard reply_guard(resp);

    return handleRedisReply(resp, "SET '" + key + "'");
}

Status RedisMetaStore::remove(const std::string &key) {
    if (!connected_) {
        return Status::MetadataError("Redis connection not available" LOC_MARK);
    }

    // Use binary-safe formatting to prevent command injection
    redisReply *resp =
        (redisReply *)redisCommand(client_, "DEL %b", key.data(), key.size());
    RedisReplyGuard reply_guard(resp);

    return handleRedisReply(resp, "DEL '" + key + "'");
}
}  // namespace tent
}  // namespace mooncake

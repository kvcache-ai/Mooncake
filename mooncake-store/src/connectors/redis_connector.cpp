#ifdef HAVE_REDIS

#include "connectors/redis_connector.h"

#include <cstdlib>
#include <glog/logging.h>

namespace mooncake {

RedisConnector::RedisConnector() : context_(nullptr), port_(6379) {
    const char* host = std::getenv("MOONCAKE_REDIS_HOST");
    const char* port = std::getenv("MOONCAKE_REDIS_PORT");
    const char* password = std::getenv("MOONCAKE_REDIS_PASSWORD");
    const char* db = std::getenv("MOONCAKE_REDIS_DB");

    host_ = host ? host : "127.0.0.1";
    port_ = port ? std::stoi(port) : 6379;

    context_ = redisConnect(host_.c_str(), port_);
    if (!context_ || context_->err) {
        std::string error = context_ ? context_->errstr : "connection failed";
        if (context_) redisFree(context_);
        throw std::runtime_error("Redis connection failed: " + error);
    }

    if (password && *password) {
        redisReply* reply =
            (redisReply*)redisCommand(context_, "AUTH %s", password);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            std::string error = reply ? reply->str : "auth failed";
            if (reply) freeReplyObject(reply);
            redisFree(context_);
            throw std::runtime_error("Redis auth failed: " + error);
        }
        freeReplyObject(reply);
    }

    if (db && *db) {
        redisReply* reply =
            (redisReply*)redisCommand(context_, "SELECT %s", db);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            std::string error = reply ? reply->str : "select db failed";
            if (reply) freeReplyObject(reply);
            redisFree(context_);
            throw std::runtime_error("Redis select db failed: " + error);
        }
        freeReplyObject(reply);
    }

    LOG(INFO) << "RedisConnector initialized: " << GetConnectionInfo();
}

RedisConnector::~RedisConnector() {
    if (context_) {
        redisFree(context_);
    }
}

tl::expected<void, std::string> RedisConnector::ListObjects(
    const std::string& prefix, std::vector<ExternalObject>& objects) {
    objects.clear();
    std::string pattern = prefix + "*";
    unsigned long cursor = 0;

    do {
        redisReply* reply = (redisReply*)redisCommand(
            context_, "SCAN %lu MATCH %s", cursor, pattern.c_str());
        if (!reply || reply->type != REDIS_REPLY_ARRAY) {
            std::string error = reply ? "invalid reply" : "null reply";
            if (reply) freeReplyObject(reply);
            return tl::make_unexpected(error);
        }

        cursor = std::stoul(reply->element[0]->str);
        for (size_t i = 0; i < reply->element[1]->elements; ++i) {
            objects.push_back({reply->element[1]->element[i]->str, 0, ""});
        }
        freeReplyObject(reply);
    } while (cursor != 0);

    return {};
}

tl::expected<void, std::string> RedisConnector::DownloadObject(
    const std::string& key, std::vector<uint8_t>& buffer) {
    redisReply* reply =
        (redisReply*)redisCommand(context_, "GET %s", key.c_str());
    if (!reply) {
        return tl::make_unexpected("Redis GET failed");
    }
    if (reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(reply);
        return tl::make_unexpected("Key not found: " + key);
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        std::string error = reply->str ? reply->str : "unknown Redis error";
        freeReplyObject(reply);
        return tl::make_unexpected("Redis GET error for key '" + key +
                                   "': " + error);
    }
    if (reply->type != REDIS_REPLY_STRING) {
        freeReplyObject(reply);
        return tl::make_unexpected("Invalid reply type");
    }

    buffer.assign(reply->str, reply->str + reply->len);
    freeReplyObject(reply);
    return {};
}

std::string RedisConnector::GetConnectionInfo() const {
    return "RedisConnector: host=" + host_ + ", port=" + std::to_string(port_);
}

}  // namespace mooncake

#endif  // HAVE_REDIS

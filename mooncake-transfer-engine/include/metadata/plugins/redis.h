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

#ifndef METADATA_PLUGIN_REDIS_H
#define METADATA_PLUGIN_REDIS_H

#include <glog/logging.h>
#include <hiredis/hiredis.h>

#include <atomic>

#include "common/common.h"
#include "metadata/plugin.h"

namespace mooncake {
class RedisMetadataPlugin : public MetadataPlugin {
   public:
    RedisMetadataPlugin() {}

    virtual ~RedisMetadataPlugin() { disconnect(); }

    virtual Status connect(const std::string &endpoint) {
        if (connected_) {
            return Status::Metadata("redis: connection has been established");
        }
        auto hostname_port = parseHostNameWithPort(endpoint);
        client_ =
            redisConnect(hostname_port.first.c_str(), hostname_port.second);
        if (!client_ || client_->err) {
            std::string message = "redis: connect \'" + endpoint +
                                  "\' failed: " + client_->errstr;
            client_ = nullptr;
            return Status::Metadata(message);
        }
        connected_ = true;
        return Status::OK();
    }

    Status disconnect() {
        if (connected_) {
            redisFree(client_);
            connected_ = false;
        }
        return Status::OK();
    }

    virtual Status get(const std::string &key, std::string &value) {
        if (!connected_) {
            return Status::Metadata("redis: connection not available");
        }
        redisReply *resp =
            (redisReply *)redisCommand(client_, "GET %s", key.c_str());
        if (!resp || resp->type == REDIS_REPLY_ERROR) {
            std::string message = "redis: get \'" + key + "\' failed: " +
                                  (resp->str ? resp->str : "n/a");
            return Status::Metadata(message);
        }
        if (!resp->str) {
            freeReplyObject(resp);
            return Status::NotSuchKey();
        }
        value = std::string(resp->str);
        freeReplyObject(resp);
        return Status::OK();
    }

    virtual Status set(const std::string &key, const std::string &value) {
        if (!connected_) {
            return Status::Metadata("redis: connection not available");
        }
        redisReply *resp = (redisReply *)redisCommand(
            client_, "SET %s %s", key.c_str(), value.c_str());
        if (!resp || resp->type == REDIS_REPLY_ERROR) {
            std::string message = "redis: set \'" + key + "\' failed: " +
                                  (resp->str ? resp->str : "n/a");
            return Status::Metadata(message);
        }
        return Status::OK();
    }

    virtual Status remove(const std::string &key) {
        if (!connected_) {
            return Status::Metadata("redis: connection not available");
        }
        redisReply *resp =
            (redisReply *)redisCommand(client_, "DEL %s", key.c_str());
        if (!resp || resp->type == REDIS_REPLY_ERROR) {
            std::string message = "redis: remove \'" + key + "\' failed: " +
                                  (resp->str ? resp->str : "n/a");
            return Status::Metadata(message);
        }
        return Status::OK();
    }

   private:
    std::atomic<bool> connected_;
    redisContext *client_;
};
}  // namespace mooncake

#endif  // METADATA_PLUGIN_REDIS_H
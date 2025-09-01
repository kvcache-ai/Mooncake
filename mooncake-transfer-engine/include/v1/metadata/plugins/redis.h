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

#include "v1/metadata/plugin.h"
#include "v1/utility/ip.h"

namespace mooncake {
namespace v1 {
class RedisMetadataPlugin : public MetadataPlugin {
   public:
    RedisMetadataPlugin() {}

    virtual ~RedisMetadataPlugin() { disconnect(); }

    virtual Status connect(const std::string &endpoint) {
        if (connected_) {
            return Status::MetadataError(
                "Redis connection already established" LOC_MARK);
        }
        auto hostname_port = parseHostNameWithPort(endpoint, 6379);
        client_ =
            redisConnect(hostname_port.first.c_str(), hostname_port.second);
        if (!client_ || client_->err) {
            std::string message =
                "Redis cannot connect \'" + endpoint + "\': " + client_->errstr;
            client_ = nullptr;
            return Status::MetadataError(message + LOC_MARK);
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
            return Status::MetadataError(
                "Redis connection not available" LOC_MARK);
        }
        redisReply *resp =
            (redisReply *)redisCommand(client_, "GET %s", key.c_str());
        if (!resp || resp->type == REDIS_REPLY_ERROR) {
            std::string message = "Redis failed to get \'" + key + "\'" +
                                  (resp->str ? resp->str : "");
            return Status::MetadataError(message + LOC_MARK);
        }
        if (!resp->str) {
            freeReplyObject(resp);
            return Status::InvalidEntry(key);
        }
        value = std::string(resp->str);
        freeReplyObject(resp);
        return Status::OK();
    }

    virtual Status set(const std::string &key, const std::string &value) {
        if (!connected_) {
            return Status::MetadataError(
                "Redis connection not available" LOC_MARK);
        }
        redisReply *resp = (redisReply *)redisCommand(
            client_, "SET %s %s", key.c_str(), value.c_str());
        if (!resp || resp->type == REDIS_REPLY_ERROR) {
            std::string message = "Redis failed to set \'" + key + "\'" +
                                  (resp->str ? resp->str : "");
            return Status::MetadataError(message + LOC_MARK);
        }
        return Status::OK();
    }

    virtual Status remove(const std::string &key) {
        if (!connected_) {
            return Status::MetadataError(
                "Redis connection not available" LOC_MARK);
        }
        redisReply *resp =
            (redisReply *)redisCommand(client_, "DEL %s", key.c_str());
        if (!resp || resp->type == REDIS_REPLY_ERROR) {
            std::string message = "Redis failed to remove \'" + key + "\'" +
                                  (resp->str ? resp->str : "");
            return Status::MetadataError(message + LOC_MARK);
        }
        return Status::OK();
    }

   private:
    std::atomic<bool> connected_;
    redisContext *client_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // METADATA_PLUGIN_REDIS_H
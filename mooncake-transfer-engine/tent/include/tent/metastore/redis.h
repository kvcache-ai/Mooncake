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

#ifndef TENT_REDIS_H
#define TENT_REDIS_H

#include <glog/logging.h>
#include <hiredis/hiredis.h>

#include <atomic>

#include "tent/runtime/metastore.h"
#include "tent/common/utils/ip.h"

namespace mooncake {
namespace tent {

// Redis configuration constants
constexpr uint8_t REDIS_MAX_DB_INDEX = 255;
constexpr uint8_t REDIS_DEFAULT_DB_INDEX = 0;
constexpr int REDIS_DEFAULT_PORT = 6379;

class RedisMetaStore : public MetaStore {
   public:
    RedisMetaStore();

    virtual ~RedisMetaStore();

    virtual Status connect(const std::string &endpoint);

    Status connect(const std::string &endpoint, const std::string &password,
                   uint8_t db_index);

    Status disconnect();

    virtual Status get(const std::string &key, std::string &value);

    virtual Status set(const std::string &key, const std::string &value);

    virtual Status remove(const std::string &key);

   private:
    std::atomic<bool> connected_;
    redisContext *client_;

    // Helper function for handling Redis replies
    Status handleRedisReply(redisReply *reply,
                            const std::string &operation) const;

    // Helper function for cleaning up failed connections
    void cleanupFailedConnection();
};
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_REDIS_H
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

#ifndef METASTORE_REDIS_H
#define METASTORE_REDIS_H

#include <glog/logging.h>
#include <hiredis/hiredis.h>

#include <atomic>

#include "v1/runtime/plugin.h"
#include "v1/platform/ip.h"

namespace mooncake {
namespace v1 {
class RedisMetadataPlugin : public MetadataPlugin {
   public:
    RedisMetadataPlugin();

    virtual ~RedisMetadataPlugin();

    virtual Status connect(const std::string &endpoint);

    Status disconnect();

    virtual Status get(const std::string &key, std::string &value);

    virtual Status set(const std::string &key, const std::string &value);

    virtual Status remove(const std::string &key);

   private:
    std::atomic<bool> connected_;
    redisContext *client_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // METASTORE_REDIS_H
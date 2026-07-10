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

#include "tent/runtime/metastore.h"
#ifdef USE_ETCD
#include "tent/metastore/etcd.h"
#endif
#ifdef USE_REDIS
#include "tent/metastore/redis.h"
#endif
#ifdef USE_HTTP
#include "tent/metastore/http.h"
#endif

#include <glog/logging.h>

namespace mooncake {
namespace tent {

std::shared_ptr<MetaStore> MetaStore::Create(const std::string& type,
                                             const std::string& servers) {
    std::shared_ptr<MetaStore> plugin;
#ifdef USE_ETCD
    if (type == "etcd") {
        plugin = std::make_shared<EtcdMetaStore>();
    }
#endif  // USE_ETCD
#ifdef USE_REDIS
    if (type == "redis") {
        // Get Redis password from environment variable for security
        std::string password;
        const char* env_password = std::getenv("MC_REDIS_PASSWORD");
        if (env_password && *env_password) {
            password = env_password;
        }

        std::string username;
        const char* env_username = std::getenv("MC_REDIS_USERNAME");
        if (env_username && *env_username) {
            username = env_username;
        }

        // Get Redis DB index from environment variable
        int redis_db_index = REDIS_DEFAULT_DB_INDEX;
        const char* env_db_index = std::getenv("MC_REDIS_DB_INDEX");
        if (env_db_index && *env_db_index) {
            try {
                redis_db_index = std::stoi(env_db_index);
            } catch (const std::exception& e) {
                LOG(WARNING)
                    << "Invalid MC_REDIS_DB_INDEX environment variable: "
                    << env_db_index << ", using default "
                    << static_cast<int>(REDIS_DEFAULT_DB_INDEX);
            }
        }

        // Validate redis_db_index range (0-255)
        uint8_t db_index = REDIS_DEFAULT_DB_INDEX;
        if (redis_db_index >= 0 && redis_db_index <= REDIS_MAX_DB_INDEX) {
            db_index = static_cast<uint8_t>(redis_db_index);
        } else {
            LOG(WARNING) << "Invalid Redis DB index: " << redis_db_index
                         << ", using default "
                         << static_cast<int>(REDIS_DEFAULT_DB_INDEX);
        }

        auto redis_plugin = std::make_shared<RedisMetaStore>();
        auto status =
            redis_plugin->connect(servers, username, password, db_index);
        if (status.ok())
            return redis_plugin;
        else {
            LOG(FATAL) << status.ToString();
            return nullptr;
        }
    }
#endif  // USE_REDIS

#ifdef USE_HTTP
    if (type == "http") {
        plugin = std::make_shared<HttpMetaStore>();
    }
#endif  // USE_HTTP
    if (!plugin) {
        LOG(FATAL) << "Protocol " << type
                   << " not installed. Please rebuild the package.";
        return nullptr;
    }
    auto status = plugin->connect(servers);
    if (status.ok())
        return plugin;
    else {
        LOG(FATAL) << status.ToString();
        return nullptr;
    }
}
}  // namespace tent
}  // namespace mooncake

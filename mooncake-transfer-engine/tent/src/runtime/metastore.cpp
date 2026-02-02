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
std::shared_ptr<MetaStore> MetaStore::Create(const std::string &type,
                                             const std::string &servers) {
    return Create(type, servers, "", 0);
}

std::shared_ptr<MetaStore> MetaStore::Create(const std::string &type,
                                             const std::string &servers,
                                             const std::string &password,
                                             uint8_t db_index) {
    std::shared_ptr<MetaStore> plugin;
#ifdef USE_ETCD
    if (type == "etcd") {
        plugin = std::make_shared<EtcdMetaStore>();
    }
#endif  // USE_ETCD
#ifdef USE_REDIS
    if (type == "redis") {
        auto redis_plugin = std::make_shared<RedisMetaStore>();
        auto status = redis_plugin->connect(servers, password, db_index);
        if (status.ok())
            return redis_plugin;
        else {
            // Sanitize error message to ensure no password leakage
            std::string error_msg = status.ToString();
            // Check if error message might contain password-related info and sanitize
            if (error_msg.find("AUTH") != std::string::npos ||
                error_msg.find("authentication") != std::string::npos ||
                error_msg.find("password") != std::string::npos) {
                LOG(ERROR) << "Failed to connect to Redis metastore: authentication failed (server="
                           << servers << ", db=" << (int)db_index << ")";
            } else {
                LOG(ERROR) << "Failed to connect to Redis metastore: " << error_msg
                           << " (server=" << servers << ", db=" << (int)db_index << ")";
            }
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
        LOG(ERROR) << "Protocol " << type
                   << " not installed. Please rebuild the package.";
        return nullptr;
    }
    auto status = plugin->connect(servers);
    if (status.ok())
        return plugin;
    else {
        LOG(ERROR) << "Failed to connect to metastore: " << status.ToString();
        return nullptr;
    }
}
}  // namespace tent
}  // namespace mooncake

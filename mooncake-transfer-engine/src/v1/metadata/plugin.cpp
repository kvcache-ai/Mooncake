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

#include "v1/metadata/plugin.h"

#ifdef USE_REDIS
#include "v1/metadata/plugins/redis.h"
#endif

#ifdef USE_HTTP
#include "v1/metadata/plugins/http.h"
#endif

#ifdef USE_ETCD
#include "v1/metadata/plugins/etcd.h"
#endif
#include <glog/logging.h>

namespace mooncake {
namespace v1 {
std::shared_ptr<MetadataPlugin> MetadataPlugin::Create(
    const std::string &type, const std::string &servers) {
    std::shared_ptr<MetadataPlugin> plugin;
#ifdef USE_ETCD
    if (type == "etcd") {
        plugin = std::make_shared<EtcdMetadataPlugin>();
    }
#endif  // USE_ETCD
#ifdef USE_REDIS
    if (type == "redis") {
        plugin = std::make_shared<RedisMetadataPlugin>();
    }
#endif  // USE_REDIS

#ifdef USE_HTTP
    if (type == "http") {
        plugin = std::make_shared<HttpMetadataPlugin>();
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
}  // namespace v1
}  // namespace mooncake
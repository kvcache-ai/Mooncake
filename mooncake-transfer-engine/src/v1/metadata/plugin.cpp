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
static std::pair<std::string, std::string> parseConnectionString(
    const std::string &conn_string) {
    std::pair<std::string, std::string> result;
    std::string proto = "etcd";
    std::string domain;
    std::size_t pos = conn_string.find("://");

    if (pos != std::string::npos) {
        proto = conn_string.substr(0, pos);
        domain = conn_string.substr(pos + 3);
    } else {
        domain = conn_string;
    }

    result.first = proto;
    result.second = domain;
    return result;
}

std::shared_ptr<MetadataPlugin> MetadataPlugin::Create(
    const std::string &connection_string) {
    auto [proto, endpoint] = parseConnectionString(connection_string);
    std::shared_ptr<MetadataPlugin> plugin;
#ifdef USE_ETCD
    if (proto == "etcd") {
        plugin = std::make_shared<EtcdMetadataPlugin>();
    }
#endif  // USE_ETCD
#ifdef USE_REDIS
    if (proto == "redis") {
        plugin = std::make_shared<RedisMetadataPlugin>();
    }
#endif  // USE_REDIS

#ifdef USE_HTTP
    if (proto == "http" || proto == "https") {
        plugin = std::make_shared<HttpMetadataPlugin>();
        endpoint = connection_string;
    }
#endif  // USE_HTTP
    if (!plugin) {
        LOG(FATAL) << "Protocol " << proto
                   << " not installed. Please rebuild the package.";
        return nullptr;
    }
    auto status = plugin->connect(endpoint);
    if (status.ok())
        return plugin;
    else {
        LOG(FATAL) << status.ToString();
        return nullptr;
    }
}
}  // namespace v1
}  // namespace mooncake
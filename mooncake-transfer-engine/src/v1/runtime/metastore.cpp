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

#ifdef USE_DYNAMIC_LOADER
#include "v1/runtime/metastore.h"
#include <glog/logging.h>
#include <dlfcn.h>
#include <glog/logging.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace mooncake {
namespace v1 {
std::shared_ptr<MetaStore> MetaStore::Create(const std::string& type,
                                             const std::string& servers) {
    std::shared_ptr<MetaStore> plugin;
    static std::vector<void*> g_handles;
    std::string metastore_path;
    if (getenv("MOONCAKE_PLUGIN_ROOT"))
        metastore_path = std::string(getenv("MOONCAKE_PLUGIN_ROOT")) + "/";
    metastore_path += "metastore/lib" + type + "_shared.so";

    void* handle = dlopen(metastore_path.c_str(), RTLD_LAZY);
    if (!handle) {
        LOG(FATAL) << "Cannot load library: " << dlerror();
        return nullptr;
    }

    static bool registered = false;
    if (!registered) {
        atexit([]() {
            for (void* h : g_handles) {
                if (h) dlclose(h);
            }
        });
        registered = true;
    }
    g_handles.push_back(handle);

    dlerror();
    typedef MetaStore* (*allocate_plugin_t)();
    allocate_plugin_t allocate_plugin =
        (allocate_plugin_t)dlsym(handle, "allocate_plugin");
    const char* dlsym_error = dlerror();
    if (dlsym_error) {
        LOG(FATAL) << "Cannot load symbol allocate_plugin: " << dlsym_error;
        return nullptr;
    }

    plugin.reset(allocate_plugin());
    auto status = plugin->connect(servers);
    if (!status.ok()) {
        LOG(FATAL) << status.ToString();
        return nullptr;
    }

    return plugin;
}
}  // namespace v1
}  // namespace mooncake
#else
#include "v1/runtime/metastore.h"
#ifdef USE_ETCD
#include "v1/metastore/etcd.h"
#endif
#ifdef USE_REDIS
#include "v1/metastore/redis.h"
#endif
#ifdef USE_HTTP
#include "v1/metastore/http.h"
#endif

#include <glog/logging.h>

namespace mooncake {
namespace v1 {
std::shared_ptr<MetaStore> MetaStore::Create(const std::string &type,
                                             const std::string &servers) {
    std::shared_ptr<MetaStore> plugin;
#ifdef USE_ETCD
    if (type == "etcd") {
        plugin = std::make_shared<EtcdMetaStore>();
    }
#endif  // USE_ETCD
#ifdef USE_REDIS
    if (type == "redis") {
        plugin = std::make_shared<RedisMetaStore>();
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
}  // namespace v1
}  // namespace mooncake
#endif
// Copyright 2024 KVCache.AI
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
#include "v1/runtime/platform.h"
#include "v1/platform/cpu.h"
#include <dlfcn.h>
#include <glog/logging.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace mooncake {
namespace v1 {

Platform* loadDynamicPlatformLibrary(std::shared_ptr<ConfigManager> conf) {
    static void* g_handle;
    std::string platform_path;
    if (getenv("MOONCAKE_PLUGIN_ROOT"))
        platform_path = std::string(getenv("MOONCAKE_PLUGIN_ROOT")) + "/";
    platform_path +=
        "platform/lib" + conf->get("platform_name", "cuda") + "_shared.so";

    g_handle = dlopen(platform_path.c_str(), RTLD_LAZY);
    if (!g_handle) {
        LOG(FATAL) << "Cannot load library: " << dlerror();
        return nullptr;
    }

    static bool registered = false;
    if (!registered) {
        atexit([]() {
            if (g_handle) dlclose(g_handle);
        });
        registered = true;
    }

    dlerror();
    typedef Platform* (*allocate_plugin_t)(std::shared_ptr<ConfigManager>);
    allocate_plugin_t allocate_plugin =
        (allocate_plugin_t)dlsym(g_handle, "allocate_plugin");
    const char* dlsym_error = dlerror();
    if (dlsym_error) {
        LOG(FATAL) << "Cannot load symbol allocate_plugin: " << dlsym_error;
        return nullptr;
    }

    return allocate_plugin(conf);
}

Platform& Platform::getLoader(std::shared_ptr<ConfigManager> conf) {
    static std::shared_ptr<Platform> g_instance;
    if (!g_instance) {
        auto platform = loadDynamicPlatformLibrary(conf);
        if (platform) {
            g_instance.reset(platform);
        } else {
            g_instance = std::make_shared<CpuPlatform>(conf);
        }
    }
    return *g_instance;
}
}  // namespace v1
}  // namespace mooncake
#else
#include "v1/runtime/platform.h"

#ifdef USE_CUDA
#include "v1/platform/cuda.h"
#else
#include "v1/platform/cpu.h"
#endif

namespace mooncake {
namespace v1 {

Platform& Platform::getLoader(std::shared_ptr<ConfigManager> conf) {
    static std::shared_ptr<Platform> g_instance;
    if (!g_instance) {
#ifdef USE_CUDA
        g_instance = std::make_shared<CudaPlatform>(conf);
#else
        g_instance = std::make_shared<CpuPlatform>(conf);
#endif
    }
    return *g_instance;
}
}  // namespace v1
}  // namespace mooncake
#endif
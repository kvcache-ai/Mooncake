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

#include "v1/runtime/loader.h"

#include <glog/logging.h>
#include <dlfcn.h>
#include <glog/logging.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace mooncake {
namespace v1 {
Loader::Loader() {
    std::atexit([]() { Loader::instance().unloadPlugins(); });
}

Loader::~Loader() {}

Status Loader::getPlugin(const std::string& path, void*& init_func,
                         void*& exit_func) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!handle_map_.count(path)) {
        void* handle = dlopen(path.c_str(), RTLD_LAZY | RTLD_GLOBAL);
        if (!handle) {
            LOG(ERROR) << "Cannot load library: " << dlerror();
            return Status::InternalError("Cannot load library");
        }
        handle_map_[path] = handle;
    }
    void* handle = handle_map_[path];
    init_func = dlsym(handle, "plugin_init");
    if (!init_func) {
        LOG(ERROR) << "Cannot get address of plugin_init: " << dlerror();
        dlclose(handle);
        return Status::InternalError("Cannot get address of plugin_init");
    }
    exit_func = dlsym(handle, "plugin_exit");
    if (!exit_func) {
        LOG(ERROR) << "Cannot get address of plugin_exit: " << dlerror();
        dlclose(handle);
        return Status::InternalError("Cannot get address of plugin_exit");
    }
    return Status::OK();
}

Status Loader::unloadPlugins() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& entry : handle_map_) {
        dlclose(entry.second);
    }
    handle_map_.clear();
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake

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

#ifndef LOADER_H
#define LOADER_H

#include "v1/common/status.h"

#include <mutex>
#include <memory>
#include <glog/logging.h>
#include <unordered_map>

namespace mooncake {
namespace v1 {
class Loader {
   public:
    Loader();

    ~Loader();

    static Loader& instance() {
        static Loader instance;
        return instance;
    }

    template <typename T, typename... Args>
    std::shared_ptr<T> loadPlugin(const std::string& category,
                                  const std::string& type, Args&&... args) {
        std::string path;
        if (const char* root = getenv("MOONCAKE_PLUGIN_ROOT")) {
            path = std::string(root) + "/";
        }
        path += category + "/lib" + category + "_" + type + "_shared.so";

        void* init_ptr = nullptr;
        void* exit_ptr = nullptr;

        auto status = getPlugin(path, init_ptr, exit_ptr);
        if (!status.ok()) return nullptr;

        std::shared_ptr<T> plugin;
        if constexpr (sizeof...(Args) == 0) {
            auto plugin_init = reinterpret_cast<T* (*)()>(init_ptr);
            auto plugin_exit = reinterpret_cast<void (*)(T*)>(exit_ptr);
            plugin.reset(plugin_init(),
                         [plugin_exit](T* p) { plugin_exit(p); });
        } else {
            auto plugin_init = reinterpret_cast<T* (*)(Args...)>(init_ptr);
            auto plugin_exit = reinterpret_cast<void (*)(T*)>(exit_ptr);
            plugin.reset(plugin_init(std::forward<Args>(args)...),
                         [plugin_exit](T* p) { plugin_exit(p); });
        }

        return plugin;
    }

   private:
    Status getPlugin(const std::string& path, void*& init_func,
                     void*& exit_func);

    Status unloadPlugins();

   private:
    std::mutex mutex_;
    std::unordered_map<std::string, void*> handle_map_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // LOADER_H
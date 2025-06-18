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

#ifndef CONFIG_MANAGER_H
#define CONFIG_MANAGER_H

#include <v1/common/status.h>
#include <jsoncpp/json/json.h>

#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>

namespace mooncake {
namespace v1 {
class ConfigManager {
   public:
    static ConfigManager& Instance() {
        static ConfigManager instance;
        return instance;
    }

    Status loadConfig(const std::filesystem::path& config_path);

    template <typename T>
    T get(const std::string& key_path, const T& default_value) const;

    template <typename T>
    std::vector<T> getArray(const std::string& key_path) const;

    void reload() { loadConfig(config_path_); }

   private:
    ConfigManager() = default;

    Json::Value config_data_;
    std::filesystem::path config_path_;
    mutable std::mutex mutex_;

    const Json::Value* findValue(const std::string& key_path) const;
};

template <typename T>
std::vector<T> ConfigManager::getArray(const std::string& key_path) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (const Json::Value* val = findValue(key_path)) {
        if (val->isArray()) {
            std::vector<int> result;
            for (const auto& item : *val) {
                if (item.isInt()) {
                    result.push_back(item.asInt());
                } else {
                    return {};
                }
            }
            return result;
        }
    }
    return {};
}
}  // namespace v1
}  // namespace mooncake

#endif  // CONFIG_MANAGER_H
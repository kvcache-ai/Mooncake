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

#include <v1/thirdparty/nlohmann/json.h>
#include <v1/common/status.h>
#include <v1/common/types.h>

#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>

namespace mooncake {
namespace v1 {
using json = nlohmann::json;

class ConfigManager {
    static const char kDelimiter = '/';

   public:
    Status loadConfig(const std::filesystem::path& config_path);

    Status loadConfigContent(const std::string& content);

    template <typename T>
    T get(const std::string& key_path, const T& default_value) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const json* val = findValue(key_path);
        if (val && !val->is_null()) {
            try {
                return val->get<T>();
            } catch (...) {
                return default_value;
            }
        }
        return default_value;
    }

    std::string get(const std::string& key, const char* def) const {
        return get<std::string>(key, std::string(def));
    }

    template <typename T>
    std::vector<T> getArray(const std::string& key) const {
        return get<std::vector<T>>(key, {});
    }

    template <typename T>
    void set(const std::string& key_path, const T& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        json* target = &config_data_;
        size_t start = 0, end = key_path.find(kDelimiter);
        while (end != std::string::npos) {
            std::string part = key_path.substr(start, end - start);
            target = &((*target)[part]);
            start = end + 1;
            end = key_path.find(kDelimiter, start);
        }
        std::string last = key_path.substr(start);
        (*target)[last] = value;
    }

    std::string dump(int indent = 2) const;

    Status save(const std::filesystem::path& out_path) const;

   private:
    json config_data_;
    std::filesystem::path config_path_;
    mutable std::mutex mutex_;

    const json* findValue(const std::string& key_path) const;
};
}  // namespace v1
}  // namespace mooncake

#endif  // CONFIG_MANAGER_H
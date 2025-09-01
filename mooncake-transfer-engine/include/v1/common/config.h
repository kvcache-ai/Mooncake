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

#include <jsoncpp/json/json.h>
#include <v1/common/status.h>
#include <v1/common/types.h>

#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>

namespace mooncake {
namespace v1 {
class ConfigManager {
   public:
    Status loadConfig(const std::filesystem::path& config_path);

    Status loadConfigContent(const std::string& content);

    void set(const std::string& key_path, const std::string& value);

    std::string get(const std::string& key_path,
                    const char* default_value) const {
        return get(key_path, std::string(default_value));
    }

    std::string get(const std::string& key_path,
                    const std::string& default_value) const;

    int get(const std::string& key_path, int default_value) const;

    uint32_t get(const std::string& key_path, uint32_t default_value) const;

    uint64_t get(const std::string& key_path, uint64_t default_value) const;

    double get(const std::string& key_path, double default_value) const;

    bool get(const std::string& key_path, bool default_value) const;

    std::vector<std::string> getArray(const std::string& key_path) const;

    std::vector<int> getArrayInt(const std::string& key_path) const;

    void reload() { loadConfig(config_path_); }

   private:
    Json::Value config_data_;
    std::filesystem::path config_path_;
    std::unordered_map<std::string, std::string> mutable_entries_;
    mutable std::mutex mutex_;

    const Json::Value* findValue(const std::string& key_path) const;
};
}  // namespace v1
}  // namespace mooncake

#endif  // CONFIG_MANAGER_H
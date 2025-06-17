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

#ifndef CONFIG_H
#define CONFIG_H

#include <common/status.h>
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

Status ConfigManager::loadConfig(const std::filesystem::path& config_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    config_path_ = config_path;

    try {
        if (std::filesystem::exists(config_path)) {
            std::ifstream file(config_path);
            Json::CharReaderBuilder readerBuilder;
            std::string errs;
            if (!Json::parseFromStream(readerBuilder, file, &config_data_,
                                       &errs)) {
                return Status::MalformedJson("Json parse error: " + errs + MSG_TAIL);
            }
            return Status::OK();
        } else {
            return Status::InvalidArgument("Unable to load config file" + MSG_TAIL);
        }
    } catch (const std::exception& e) {
        return Status::InternalError("Detected C++ exception: " + e.what() + MSG_TAIL);
    }
}

const Json::Value* ConfigManager::findValue(const std::string& key_path) const {
    size_t start = 0;
    size_t end = key_path.find('/');
    const Json::Value* current = &config_data_;

    while (end != std::string::npos) {
        std::string part = key_path.substr(start, end - start);
        if (!current->isObject() || !current->isMember(part)) return nullptr;

        current = &((*current)[part]);
        start = end + 1;
        end = key_path.find('/', start);
    }

    std::string lastKey = key_path.substr(start);
    if (current->isObject() && current->isMember(lastKey)) {
        return &((*current)[lastKey]);
    }

    return nullptr;
}

template <typename T>
T ConfigManager::get(const std::string& key_path,
                     const T& default_value) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (const Json::Value* val = findValue(key_path)) {
        try {
            if (val->isConvertibleTo(Json::ValueType::intValue)) {
                return val->asInt();
            } else if (val->isConvertibleTo(Json::ValueType::realValue)) {
                return val->asFloat();
            } else if (val->isConvertibleTo(Json::ValueType::booleanValue)) {
                return val->asBool();
            } else if (val->isString()) {
                return val->asString();
            }
        } catch (...) {
            return default_value;
        }
    }
    return default_value;
}

template <typename T>
std::vector<T> ConfigManager::getArray(const std::string& key_path) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (const Json::Value* val = FindValue(key_path)) {
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

#endif  // CONFIG_H
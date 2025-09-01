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

#include "v1/common/config.h"

namespace mooncake {
namespace v1 {
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
                std::string msg = "Json parse error: " + errs + LOC_MARK;
                return Status::MalformedJson(msg);
            }
            return Status::OK();
        } else {
            std::string msg = "Unable to load config file" LOC_MARK;
            return Status::InvalidArgument(msg);
        }
    } catch (const std::exception& e) {
        std::string msg =
            "Detected C++ exception: " + std::string(e.what()) + LOC_MARK;
        return Status::InternalError(msg);
    }
}

Status ConfigManager::loadConfigContent(const std::string& content) {
    std::lock_guard<std::mutex> lock(mutex_);
    Json::Reader reader;
    if (content.empty() || !reader.parse(content, config_data_)) {
        fprintf(stderr, "OK %s\n", content.c_str());
        return Status::MalformedJson(
            "Unrecognized format of json content" LOC_MARK);
    }
    return Status::OK();
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

void ConfigManager::set(const std::string& key_path, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    mutable_entries_[key_path] = value;
}

std::string ConfigManager::get(const std::string& key_path,
                               const std::string& default_value) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (mutable_entries_.count(key_path)) return mutable_entries_.at(key_path);
    if (const Json::Value* val = findValue(key_path)) {
        try {
            if (val && val->isString()) return val->asString();
        } catch (...) {
            return default_value;
        }
    }
    return default_value;
}

int ConfigManager::get(const std::string& key_path, int default_value) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (mutable_entries_.count(key_path)) {
        try {
            auto value = mutable_entries_.at(key_path);
            return std::stoi(value);
        } catch (...) {
            return default_value;
        }
    }
    if (const Json::Value* val = findValue(key_path)) {
        try {
            if (val && val->isConvertibleTo(Json::ValueType::intValue))
                return val->asInt();
        } catch (...) {
            return default_value;
        }
    }
    return default_value;
}

uint32_t ConfigManager::get(const std::string& key_path,
                            uint32_t default_value) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (mutable_entries_.count(key_path)) {
        try {
            auto value = mutable_entries_.at(key_path);
            return std::stoul(value);
        } catch (...) {
            return default_value;
        }
    }
    if (const Json::Value* val = findValue(key_path)) {
        try {
            if (val && val->isConvertibleTo(Json::ValueType::intValue))
                return val->asUInt();
        } catch (...) {
            return default_value;
        }
    }
    return default_value;
}

uint64_t ConfigManager::get(const std::string& key_path,
                            uint64_t default_value) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (mutable_entries_.count(key_path)) {
        try {
            auto value = mutable_entries_.at(key_path);
            return std::stoull(value);
        } catch (...) {
            return default_value;
        }
    }
    if (const Json::Value* val = findValue(key_path)) {
        try {
            if (val && val->isConvertibleTo(Json::ValueType::intValue))
                return val->asUInt64();
        } catch (...) {
            return default_value;
        }
    }
    return default_value;
}

double ConfigManager::get(const std::string& key_path,
                          double default_value) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (mutable_entries_.count(key_path)) {
        try {
            auto value = mutable_entries_.at(key_path);
            return std::stod(value);
        } catch (...) {
            return default_value;
        }
    }
    if (const Json::Value* val = findValue(key_path)) {
        try {
            if (val && val->isConvertibleTo(Json::ValueType::realValue))
                return val->asDouble();
        } catch (...) {
            return default_value;
        }
    }
    return default_value;
}

bool ConfigManager::get(const std::string& key_path, bool default_value) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (mutable_entries_.count(key_path)) {
        return mutable_entries_.at(key_path) == "true";
    }
    if (const Json::Value* val = findValue(key_path)) {
        try {
            if (val && val->isConvertibleTo(Json::ValueType::booleanValue))
                return val->asBool();
        } catch (...) {
            return default_value;
        }
    }
    return default_value;
}

std::vector<std::string> ConfigManager::getArray(
    const std::string& key_path) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (mutable_entries_.count(key_path)) {
        std::vector<std::string> result;
        auto rep = mutable_entries_.at(key_path);
        Json::Value root;
        Json::Reader reader;
        try {
            if (reader.parse(rep, root) && root.isArray())
                for (const auto& item : root) result.push_back(item.asString());
        } catch (...) {
            return {};
        }
        return result;
    }

    if (const Json::Value* val = findValue(key_path)) {
        if (val->isArray()) {
            std::vector<std::string> result;
            for (const auto& item : *val) {
                try {
                    if (val->isString()) result.push_back(item.asString());
                } catch (...) {
                    return {};
                }
            }
            return result;
        }
    }
    return {};
}

std::vector<int> ConfigManager::getArrayInt(const std::string& key_path) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (mutable_entries_.count(key_path)) {
        std::vector<int> result;
        auto rep = mutable_entries_.at(key_path);
        Json::Value root;
        Json::Reader reader;
        try {
            if (reader.parse(rep, root) && root.isArray())
                for (const auto& item : root) result.push_back(item.asInt());
        } catch (...) {
            return {};
        }
        return result;
    }

    if (const Json::Value* val = findValue(key_path)) {
        if (val->isArray()) {
            std::vector<int> result;
            for (const auto& item : *val) {
                try {
                    if (val->isConvertibleTo(Json::ValueType::intValue))
                        result.push_back(item.asInt());
                } catch (...) {
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
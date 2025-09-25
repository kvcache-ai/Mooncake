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
    std::ifstream ifs(config_path);
    if (!ifs.is_open()) {
        return Status::InvalidArgument("Failed to open config file" LOC_MARK);
    }
    try {
        ifs >> config_data_;
        config_path_ = config_path;
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(std::string("Invalid JSON: ") +
                                       e.what() + LOC_MARK);
    }
}

Status ConfigManager::loadConfigContent(const std::string& content) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        config_data_ = json::parse(content);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(std::string("Invalid JSON: ") +
                                       e.what() + LOC_MARK);
    }
}

std::string ConfigManager::dump(int indent) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return config_data_.dump(indent);
}

Status ConfigManager::save(const std::filesystem::path& out_path) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::ofstream ofs(out_path);
    if (!ofs.is_open()) {
        return Status::InvalidArgument("Failed to open file for writing" +
                                       out_path.string() + LOC_MARK);
    }
    ofs << config_data_.dump(2);
    return Status::OK();
}

const json* ConfigManager::findValue(const std::string& key_path) const {
    const json* current = &config_data_;
    size_t start = 0, end = key_path.find(kDelimiter);
    while (end != std::string::npos) {
        std::string part = key_path.substr(start, end - start);
        if (!current->contains(part)) return nullptr;
        current = &((*current)[part]);
        start = end + 1;
        end = key_path.find(kDelimiter, start);
    }
    std::string last = key_path.substr(start);
    return current->contains(last) ? &((*current)[last]) : nullptr;
}
}  // namespace v1
}  // namespace mooncake
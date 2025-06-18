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

#include "v1/common/config_manager.h"

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
}  // namespace v1
}  // namespace mooncake
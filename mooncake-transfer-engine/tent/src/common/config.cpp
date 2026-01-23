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

#include "tent/common/config.h"

namespace mooncake {
namespace tent {
Status Config::load(const std::string& content) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        config_data_ = json::parse(content);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(std::string("Invalid JSON: ") +
                                       e.what() + LOC_MARK);
    }
}

std::string Config::dump(int indent) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return config_data_.dump(indent);
}

static inline void setConfig(Config& config, const std::string& env_key,
                             const std::string& config_key) {
    const char* val = std::getenv(env_key.c_str());
    if (val) {
        std::string str_val(val);
        // Try to parse as integer first
        try {
            size_t pos;
            int int_val = std::stoi(str_val, &pos);
            if (pos == str_val.length()) {
                // Fully numeric, store as int
                config.set(config_key, int_val);
                return;
            }
        } catch (const std::exception& e) {
            // Not an integer, continue to store as string
        }
        // Store as string
        config.set(config_key, str_val);
    }
}

Status ConfigHelper::loadFromEnv(Config& config) {
    const char* conf_str = std::getenv("MC_TENT_CONF");
    Status status = Status::OK();
    if (conf_str && *conf_str != '\0') {
        status = config.load(conf_str);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to parse MC_TENT_CONF: "
                         << status.ToString();
        }
    }

    // Legacy keys for backward compatibility
    setConfig(config, "MC_IB_PORT", "transports/rdma/device/port");
    setConfig(config, "MC_GID_INDEX", "transports/rdma/device/gid_index");
    return status;
}

}  // namespace tent
}  // namespace mooncake

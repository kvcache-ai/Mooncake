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

#include <glog/logging.h>

#include <sstream>

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

bool ConfigHelper::parseBool(const std::string& str, bool default_value) {
    std::string lower_str = str;
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                   ::tolower);

    if (lower_str == "true" || lower_str == "1" || lower_str == "yes" ||
        lower_str == "on") {
        return true;
    } else if (lower_str == "false" || lower_str == "0" || lower_str == "no" ||
               lower_str == "off") {
        return false;
    } else {
        LOG(WARNING) << "Invalid boolean value '" << str
                     << "', using default: " << default_value;
        return default_value;
    }
}

int ConfigHelper::parseInt(const std::string& str, int default_value) {
    try {
        return std::stoi(str);
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to parse integer '" << str << "': " << e.what()
                     << ", using default: " << default_value;
        return default_value;
    }
}

uint16_t ConfigHelper::parsePort(const std::string& str,
                                 uint16_t default_value) {
    try {
        int port = std::stoi(str);
        if (port > 0 && port <= 65535) {
            return static_cast<uint16_t>(port);
        } else {
            LOG(WARNING) << "Port " << port
                         << " out of range (1-65535), using default: "
                         << default_value;
            return default_value;
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to parse port '" << str << "': " << e.what()
                     << ", using default: " << default_value;
        return default_value;
    }
}

std::vector<double> ConfigHelper::parseDoubleArray(const std::string& str) {
    std::vector<double> result;
    std::stringstream ss(str);
    std::string item;

    while (std::getline(ss, item, ',')) {
        try {
            // Trim whitespace
            item.erase(0, item.find_first_not_of(" \t"));
            item.erase(item.find_last_not_of(" \t") + 1);

            if (!item.empty()) {
                double value = std::stod(item);
                if (value > 0) {
                    result.push_back(value);
                }
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to parse double value '" << item
                         << "': " << e.what();
        }
    }

    // Sort the result
    std::sort(result.begin(), result.end());

    return result;
}

}  // namespace tent
}  // namespace mooncake

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
        config.set(config_key, val);
    }
}

Status ConfigHelper::loadFromEnv(Config& config) {
    const char *conf_str = std::getenv("MC_TENT_CONF");
    if (conf_str) {
        config.load(conf_str);
        return Status::OK();
    }

    // Legacy keys for backward compatibility
    setConfig(config, "MC_IB_PORT", "transports/rdma/device/port");
    setConfig(config, "MC_GID_INDEX", "transports/rdma/device/gid_index");
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
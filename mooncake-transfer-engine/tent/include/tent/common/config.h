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

#ifndef TENT_CONFIG_H
#define TENT_CONFIG_H

#include <tent/thirdparty/nlohmann/json.h>
#include <tent/common/status.h>
#include <tent/common/types.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

namespace mooncake {
namespace tent {
using json = nlohmann::json;

class Config {
    static const char kDelimiter = '/';

   public:
    template <typename T>
    T get(const std::string& key_path, const T& default_value) const {
        std::lock_guard<std::mutex> lock(mutex_);
        // Try nested path lookup by splitting on kDelimiter
        const json* node = &config_data_;
        std::string::size_type start = 0;
        while (start < key_path.size()) {
            auto pos = key_path.find(kDelimiter, start);
            auto segment = key_path.substr(start, pos - start);
            auto it = node->find(segment);
            if (it == node->end()) {
                // Fallback: try flat key lookup at top level
                auto flat_it = config_data_.find(key_path);
                if (flat_it != config_data_.end() && !flat_it->is_null()) {
                    try {
                        return flat_it->get<T>();
                    } catch (...) {
                    }
                }
                return default_value;
            }
            node = &(*it);
            start = (pos == std::string::npos) ? key_path.size() : pos + 1;
        }
        if (node->is_null()) return default_value;
        try {
            return node->get<T>();
        } catch (...) {
            return default_value;
        }
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
        // Navigate/create nested path by splitting on kDelimiter
        json* node = &config_data_;
        std::string::size_type start = 0;
        while (start < key_path.size()) {
            auto pos = key_path.find(kDelimiter, start);
            auto segment = key_path.substr(start, pos - start);
            node = &(*node)[segment];
            start = (pos == std::string::npos) ? key_path.size() : pos + 1;
        }
        *node = value;
    }

    Status load(const std::string& content);

    Status loadFile(const std::string& file_path);

    std::string dump(int indent = 2) const;

   private:
    json config_data_;
    mutable std::mutex mutex_;
};

struct ConfigHelper {
    Status loadFromEnv(Config& config);

    // Common parsing utilities for environment variable values
    static bool parseBool(const std::string& str, bool default_value = false);
    static int parseInt(const std::string& str, int default_value = 0);
    static uint16_t parsePort(const std::string& str,
                              uint16_t default_value = 0);
    static std::vector<double> parseDoubleArray(const std::string& str);
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_CONFIG_H
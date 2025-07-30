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

#ifndef TOPOLOGY_H
#define TOPOLOGY_H

#include <glog/logging.h>
#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>  // Ubuntu
#else
#include <json/json.h>  // CentOS
#endif
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "common.h"

namespace mooncake {
struct TopologyEntry {
    std::string name;
    std::vector<std::string> preferred_hca;
    std::vector<std::string> avail_hca;

    Json::Value toJson() const {
        Json::Value matrix(Json::arrayValue);
        Json::Value hca_list(Json::arrayValue);
        for (auto &hca : preferred_hca) {
            hca_list.append(hca);
        }
        matrix.append(hca_list);
        hca_list.clear();
        for (auto &hca : avail_hca) {
            hca_list.append(hca);
        }
        matrix.append(hca_list);
        return matrix;
    }
};

using TopologyMatrix =
    std::unordered_map<std::string /* storage type */, TopologyEntry>;

class Topology {
   public:
    Topology();

    ~Topology();

    bool empty() const;

    void clear();

    int discover() {
        std::vector<std::string> filter;
        return discover(filter);
    }

    int discover(const std::vector<std::string> &filter);

    int parse(const std::string &topology_json);

    int disableDevice(const std::string &device_name);

    std::string toString() const;

    Json::Value toJson() const;

    int selectDevice(const std::string storage_type, int retry_count = 0);
    int selectDevice(const std::string storage_type, std::string_view hint,
                     int retry_count = 0);

    TopologyMatrix getMatrix() const { return matrix_; }

    const std::vector<std::string> &getHcaList() const { return hca_list_; }

   private:
    int resolve();

   private:
    TopologyMatrix matrix_;
    std::vector<std::string> hca_list_;

    struct ResolvedTopologyEntry {
        std::vector<int> preferred_hca;
        std::vector<int> avail_hca;
        // Maps for efficient name-to-index lookup
        std::unordered_map<std::string, int> preferred_hca_name_to_index_map_;
        std::unordered_map<std::string, int> avail_hca_name_to_index_map_;

        // Helper method for efficient name-to-index lookup
        int getHcaIndex(const std::string &hca_name) const {
            // First try to find in preferred HCA map
            auto it = preferred_hca_name_to_index_map_.find(hca_name);
            if (it != preferred_hca_name_to_index_map_.end()) {
                return it->second;
            }

            // Then try to find in available HCA map
            it = avail_hca_name_to_index_map_.find(hca_name);
            return (it != avail_hca_name_to_index_map_.end()) ? it->second : -1;
        }
    };
    std::unordered_map<std::string /* storage type */, ResolvedTopologyEntry>
        resolved_matrix_;
};

}  // namespace mooncake

#endif  // TOPOLOGY_H
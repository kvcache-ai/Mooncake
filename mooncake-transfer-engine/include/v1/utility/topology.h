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
#include <jsoncpp/json/json.h>
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "v1/common/config.h"
#include "v1/common/status.h"

namespace mooncake {
namespace v1 {
const static size_t DevicePriorityRanks = 3;

struct TopologyEntry {
    std::string name;
    int numa_node = 0;
    std::vector<std::string> device_list[DevicePriorityRanks];

    Json::Value toJson() const {
        Json::Value jout;
        Json::Value jdevice(Json::arrayValue);
        for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
            Json::Value jlist(Json::arrayValue);
            for (auto &device : device_list[rank]) jlist.append(device);
            jdevice.append(jlist);
        }
        jout["numa_node"] = numa_node;
        jout["devices"] = jdevice;
        return jout;
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

    Status discover(std::shared_ptr<ConfigManager> conf = nullptr);

    Status parse(const std::string &topology_json);

    Status disableDevice(const std::string &device_name);

    std::string toString() const;

    Json::Value toJson() const;

    Status selectDevice(int &device_id, const std::string &storage_type,
                        int retry_count = 0) {
        int rand_seed = -1;
        return selectDevice(device_id, storage_type, retry_count, rand_seed);
    }

    Status selectDevice(int &device_id, const std::string &storage_type,
                        int retry_count, int &rand_seed);

    const TopologyMatrix &getMatrix() const { return matrix_; }

    const std::vector<std::string> &getDeviceList() const {
        return rdma_device_list_;
    }

    int findDeviceID(const std::string &name) const {
        int index = 0;
        for (auto &entry : rdma_device_list_)
            if (entry == name)
                return index;
            else
                index++;
        return -1;
    }

   private:
    Status resolve();

   private:
    TopologyMatrix matrix_;
    std::vector<std::string> rdma_device_list_;

    struct ResolvedTopologyEntry {
        int numa_node;
        std::vector<int> device_list[DevicePriorityRanks];
    };

    std::unordered_map<std::string /* storage type */, ResolvedTopologyEntry>
        resolved_matrix_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // TOPOLOGY_H
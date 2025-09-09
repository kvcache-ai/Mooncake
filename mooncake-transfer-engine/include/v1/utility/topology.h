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
#include <unordered_set>

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

struct ResolvedTopologyEntry {
    int numa_node;
    std::vector<int> device_list[DevicePriorityRanks];
};

using ResolvedTopologyMatrix =
    std::unordered_map<std::string /* storage type */, ResolvedTopologyEntry>;

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

    void print() const;

    const TopologyMatrix &getMatrix() const { return matrix_; }

    const std::vector<std::string> &getDeviceList() const {
        return rdma_device_list_;
    }

    std::string getDeviceName(int device_id) const {
        if (device_id < 0 || device_id >= (int)rdma_device_list_.size())
            return "";
        return rdma_device_list_[device_id];
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

    int findDeviceNumaID(int dev_id) const;

    double findDeviceBandwidth(int dev_id) const { return 200; /* dummy */ }

    const ResolvedTopologyMatrix &getResolvedMatrix() const {
        return resolved_matrix_;
    }

    int getCudaDeviceCount() const;

    int getBestRdmaDeviceID(int cuda_dev_id) const;

   private:
    Status resolve();

   private:
    TopologyMatrix matrix_;
    std::vector<std::string> rdma_device_list_;
    ResolvedTopologyMatrix resolved_matrix_;
    std::map<int, int> cuda_to_rdma_dev_map_;
};

class RailTopology {
    const static size_t kMaxNuma = 4;

   public:
    RailTopology() = default;

    ~RailTopology() = default;

    RailTopology(const RailTopology &) = delete;
    RailTopology &operator=(const RailTopology &) = delete;

   public:
    Status load(const Topology *local, const Topology *remote,
                const std::string &rail_topo_json_path = "",
                const std::string &local_machine_id = "",
                const std::string &remote_machine_id = "");

    bool connected(int local_dev_id, int remote_dev_id);

    int findRemoteDeviceID(int local_dev_id, int dst_numa = -1);

   private:
    Status loadFromJson(const std::string &rail_topo_json_path,
                        const std::string &local_machine_id,
                        const std::string &remote_machine_id);

    Status loadFromSelf();

   private:
    const Topology *local_, *remote_;
    struct PairHash {
        std::size_t operator()(const std::pair<int, int> &p) const noexcept {
            return std::hash<int>()(p.first) ^
                   (std::hash<int>()(p.second) << 1);
        }
    };
    std::unordered_set<std::pair<int, int>, PairHash> connected_set_;
    std::unordered_map<int, int> best_mapping_[kMaxNuma];
};

int getCudaDeviceNumaID(int cuda_id);

}  // namespace v1
}  // namespace mooncake

#endif  // TOPOLOGY_H
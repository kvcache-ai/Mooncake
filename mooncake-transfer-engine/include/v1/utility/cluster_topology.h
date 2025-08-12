// Copyright 2025 KVCache.AI
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

#ifndef CLUSTER_TOPOLOGY_H
#define CLUSTER_TOPOLOGY_H

#include <string>
#include <unordered_map>
#include <vector>

#include "v1/common/config.h"
#include "v1/common/status.h"

struct Endpoint {
    int src_numa;
    int dst_numa;
    double bandwidth;
    double latency;
};

struct RdmaDevicePair {
    std::string src_dev;
    std::string dst_dev;

    bool operator==(const RdmaDevicePair& other) const {
        return src_dev == other.src_dev && dst_dev == other.dst_dev;
    }
};

struct NumaIdPair {
    int src_numa;
    int dst_numa;

    bool operator==(const NumaIdPair& other) const {
        return src_numa == other.src_numa && dst_numa == other.dst_numa;
    }
};

namespace std {
template <>
struct hash<RdmaDevicePair> {
    size_t operator()(const RdmaDevicePair& pair) const {
        return std::hash<std::string>{}(pair.src_dev) ^
               std::hash<std::string>{}(pair.dst_dev);
    }
};

template <>
struct hash<NumaIdPair> {
    size_t operator()(const NumaIdPair& pair) const {
        return std::hash<int>{}(pair.src_numa) ^
               std::hash<int>{}(pair.dst_numa);
    }
};
}  // namespace std

struct Node {
    std::unordered_map<RdmaDevicePair, Endpoint> endpoints;
    std::unordered_map<NumaIdPair, std::vector<RdmaDevicePair>>
        partition_matchings;
};

struct NodePair {
    std::string src_host;
    std::string dst_host;

    bool operator==(const NodePair& other) const {
        return src_host == other.src_host && dst_host == other.dst_host;
    }
};

namespace std {
template <>
struct hash<NodePair> {
    size_t operator()(const NodePair& pair) const {
        return std::hash<std::string>{}(pair.src_host) ^
               std::hash<std::string>{}(pair.dst_host);
    }
};
}  // namespace std

namespace mooncake {
namespace v1 {
class ClusterTopology {
   public:
    Status load(const std::string& filename);

    const Node* getNode(const std::string& src_host,
                        const std::string& dst_host) const;

    const Endpoint* getEndpoint(const std::string& src_host,
                                const std::string& src_dev,
                                const std::string& dst_host,
                                const std::string& dst_dev);

    std::string findOptionalDevice(const std::string& src_host,
                                   const std::string& src_dev,
                                   const std::string& dst_host, int dst_numa);

   private:
    struct DeviceInfo {
        int numa;
    };

   private:
    std::unordered_map<NodePair, Node> entries_;

    std::unordered_map<std::string, std::unordered_map<std::string, DeviceInfo>>
        device_list_;
};

}  // namespace v1
}  // namespace mooncake

#endif  // CLUSTER_TOPOLOGY_H
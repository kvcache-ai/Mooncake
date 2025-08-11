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

#include "v1/utility/cluster_topology.h"

#include <glog/logging.h>
#include <jsoncpp/json/json.h>

#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "v1/common/status.h"
#include "v1/memory/location.h"
#include "v1/utility/random.h"

namespace mooncake {
namespace v1 {
Status ClusterTopology::load(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
        return Status::InvalidArgument("Unable to open cluster topology file");
    }

    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errs;
    if (!Json::parseFromStream(builder, ifs, &root, &errs)) {
        return Status::MalformedJson(std::string("Failed to parse json:") +
                                     errs);
    }

    for (const auto& entry : root) {
        std::string src_host = entry["src_host"].asString();
        std::string dst_host = entry["dst_host"].asString();
        Node node;

        // Process endpoints
        for (const auto& ep : entry["endpoints"]) {
            RdmaDevicePair rdma_pair{ep["src_dev"].asString(),
                                     ep["dst_dev"].asString()};
            Endpoint e{ep["src_numa"].asInt(), ep["dst_numa"].asInt(),
                       ep["bandwidth"].asDouble(), ep["latency"].asDouble()};
            node.endpoints[rdma_pair] = e;
        }

        // Process partition matchings
        const auto& matchings = entry["partition_matchings"];
        for (const auto& key : matchings.getMemberNames()) {
            for (const auto& ep : matchings[key]) {
                NumaIdPair numa_pair{ep["src_numa"].asInt(),
                                     ep["dst_numa"].asInt()};
                RdmaDevicePair rdma_pair{ep["src_dev"].asString(),
                                         ep["dst_dev"].asString()};
                node.partition_matchings[numa_pair].push_back(rdma_pair);
            }
        }

        // Store the node in entries_
        NodePair node_pair{src_host, dst_host};
        entries_[node_pair] = node;
    }

    return Status::OK();
}

const Node* ClusterTopology::getNode(const std::string& src_host,
                                     const std::string& dst_host) const {
    NodePair node_pair{src_host, dst_host};
    if (entries_.count(node_pair)) return &entries_.at(node_pair);
    return nullptr;
}

}  // namespace v1
}  // namespace mooncake

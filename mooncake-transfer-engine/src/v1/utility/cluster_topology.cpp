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
Status ClusterTopology::load(const std::string& src_host,
                             const std::string& filename) {
    entries_.clear();
    src_dev_numa_cache_.clear();

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
        if (src_host != entry["src_host"].asString()) continue;
        std::string dst_host = entry["dst_host"].asString();

        Node node;
        // Process endpoints
        for (const auto& ep : entry["endpoints"]) {
            RdmaDevicePair rdma_pair{ep["src_dev"].asString(),
                                     ep["dst_dev"].asString()};
            Endpoint e{ep["src_numa"].asInt(), ep["dst_numa"].asInt(),
                       ep["bandwidth"].asDouble(), ep["latency"].asDouble()};
            node.endpoints[rdma_pair] = e;
            src_dev_numa_cache_[rdma_pair.src_dev] = e.src_numa;
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

        entries_[dst_host] = node;
    }

    return Status::OK();
}

const Node* ClusterTopology::getNode(const std::string& dst_host) const {
    if (entries_.count(dst_host)) return &entries_.at(dst_host);
    return nullptr;
}

const Endpoint* ClusterTopology::getEndpoint(const std::string& src_dev,
                                             const std::string& dst_host,
                                             const std::string& dst_dev) {
    auto node = getNode(dst_host);
    if (!node) return nullptr;
    RdmaDevicePair pair{src_dev, dst_dev};
    if (node && node->endpoints.count(pair)) return &node->endpoints.at(pair);
    return nullptr;
}

std::string ClusterTopology::findOptimalMapping(const std::string& src_dev,
                                                const std::string& dst_host,
                                                int dst_numa) {
    auto node = getNode(dst_host);
    if (!node || !src_dev_numa_cache_.count(src_dev)) return "";
    NumaIdPair numa_pair{src_dev_numa_cache_[src_dev], dst_numa};
    if (!node->partition_matchings.count(numa_pair)) return "";
    auto& mapping = node->partition_matchings.at(numa_pair);
    for (auto& entry : mapping)
        if (entry.src_dev == src_dev) return entry.dst_dev;
    return "";
}

}  // namespace v1
}  // namespace mooncake

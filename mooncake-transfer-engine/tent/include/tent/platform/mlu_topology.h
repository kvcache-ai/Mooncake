// Copyright 2026 KVCache.AI
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

#ifndef TENT_PLATFORM_MLU_TOPOLOGY_H_
#define TENT_PLATFORM_MLU_TOPOLOGY_H_

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <iterator>
#include <limits>
#include <string>
#include <vector>

#include "tent/runtime/topology.h"

namespace mooncake::tent::mlu_detail {

inline std::string normalizePciBusId(std::string id) {
    std::transform(id.begin(), id.end(), id.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return id;
}

inline std::filesystem::path resolvePciPath(const std::filesystem::path& root,
                                            const std::string& bus_id) {
    if (bus_id.empty()) return {};
    std::error_code error;
    auto path =
        std::filesystem::canonical(root / normalizePciBusId(bus_id), error);
    return error ? std::filesystem::path{} : path;
}

// Count hops only within a shared PCI root; sibling devices have distance zero.
// Across roots, sysfs directory depth says nothing about the host interconnect
// cost. Return unknown and let rankNics use NUMA affinity in that case.
inline int pciDistance(const std::filesystem::path& a,
                       const std::filesystem::path& b) {
    if (a.empty() || b.empty()) return -1;
    auto i = a.begin(), j = b.begin();
    bool common_pci_root = false;
    while (i != a.end() && j != b.end() && *i == *j) {
        // Linux names PCI root buses pciDDDD:BB, including on platforms
        // where they are nested below a host-controller device directory.
        auto part = i->string();
        if (part.size() == 10 && part.compare(0, 3, "pci") == 0 &&
            part[7] == ':' &&
            std::all_of(part.begin() + 3, part.begin() + 7,
                        [](unsigned char c) { return std::isxdigit(c); }) &&
            std::all_of(part.begin() + 8, part.end(),
                        [](unsigned char c) { return std::isxdigit(c); }))
            common_pci_root = true;
        ++i;
        ++j;
    }
    if (!common_pci_root) return -1;
    return std::max(0, static_cast<int>(std::distance(i, a.end()) +
                                        std::distance(j, b.end())) -
                           2);
}

inline void rankNics(
    Topology::MemEntry& mem, const std::vector<Topology::NicEntry>& nics,
    const std::filesystem::path& pci_root = "/sys/bus/pci/devices") {
    for (auto& rank : mem.device_list) rank.clear();
    auto device_path = resolvePciPath(pci_root, mem.pci_bus_id);
    std::vector<int> distances;
    int nearest = std::numeric_limits<int>::max();
    for (const auto& nic : nics) {
        int distance =
            pciDistance(device_path, resolvePciPath(pci_root, nic.pci_bus_id));
        distances.push_back(distance);
        // Missing links and unrelated PCI roots provide no PCIe affinity.
        if (distance >= 0) nearest = std::min(nearest, distance);
    }
    const bool have_distance = nearest != std::numeric_limits<int>::max();
    for (size_t id = 0; id < nics.size(); ++id) {
        const bool same_numa =
            mem.numa_node >= 0 && mem.numa_node == nics[id].numa_node;
        int rank = same_numa ? 1 : 2;
        if ((have_distance && distances[id] == nearest) ||
            (!have_distance && same_numa))
            rank = 0;
        mem.device_list[rank].push_back(id);
    }
}

}  // namespace mooncake::tent::mlu_detail

#endif  // TENT_PLATFORM_MLU_TOPOLOGY_H_

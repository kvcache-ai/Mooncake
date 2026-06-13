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

#ifndef PCI_TOPOLOGY_H
#define PCI_TOPOLOGY_H

#include <cctype>
#include <climits>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace mooncake {

enum class PciPathType {
    PIX = 0,   // Same PCI switch
    PXB = 1,   // Crossing PCI bridges
    PHB = 2,   // Crossing PCH/host bridge
    NODE = 3,  // Same NUMA node, no common PCI ancestor
    SYS = 4,   // Cross-NUMA
    DIS = 5,   // Disconnected
};

struct InfinibandDevice {
    std::string name;
    std::string pci_bus_id;
    int numa_node;
    double
        pci_bw_gbps;  // PCIe bandwidth in GT/s * width (e.g. 1024 for Gen5 x16)
};

constexpr size_t kMaxPciDepth = 32;

// Normalize a PCI bus ID: lowercase and strip leading zeros from the domain
// so that CUDA's 8-digit format (e.g. "00000008:06:00.0") matches sysfs's
// 4-digit format (e.g. "0008:06:00.0").
inline std::string normalizePciBusId(const std::string &pci_bus_id) {
    std::string normalized = pci_bus_id;
    for (char &ch : normalized) {
        ch = std::tolower(static_cast<unsigned char>(ch));
    }
    auto first_colon = normalized.find(':');
    if (first_colon != std::string::npos && first_colon > 4) {
        size_t leading = first_colon - 4;
        if (normalized.substr(0, leading) == std::string(leading, '0')) {
            normalized.erase(0, leading);
        }
    }
    return normalized;
}

// Classify the PCI path between a GPU and a NIC given their pre-built
// ancestor chains and NUMA node information.  This is the pure-logic core
// that can be unit-tested without sysfs access.
inline std::pair<PciPathType, int> classifyGpuNicPath(
    const std::vector<std::string> &gpu_ancestor_chain,
    const std::unordered_set<std::string> &gpu_ancestors, int gpu_numa_node,
    int nic_numa_node, const std::vector<std::string> &nic_ancestor_chain) {
    if (gpu_numa_node >= 0 && nic_numa_node >= 0 &&
        gpu_numa_node != nic_numa_node) {
        return {PciPathType::SYS, -1};
    }

    int nic_hops = 0;
    for (const auto &ancestor : nic_ancestor_chain) {
        if (gpu_ancestors.count(ancestor)) {
            int gpu_hops = 0;
            for (const auto &gpu_ancestor : gpu_ancestor_chain) {
                if (gpu_ancestor == ancestor) {
                    break;
                }
                gpu_hops++;
            }

            int total_hops = gpu_hops + nic_hops;
            if (total_hops <= 2) {
                return {PciPathType::PIX, total_hops};
            }
            if (total_hops <= 4) {
                return {PciPathType::PXB, total_hops};
            }
            return {PciPathType::PHB, total_hops};
        }
        nic_hops++;
    }

    if (gpu_numa_node >= 0 && gpu_numa_node == nic_numa_node) {
        // All same-NUMA NICs with no common PCI ancestor are equally close;
        // chain length is not a meaningful proximity metric here.  Return a
        // fixed hop count so the selection logic treats them identically.
        return {PciPathType::NODE, 0};
    }
    return {PciPathType::SYS, -1};
}

struct HcaPartition {
    std::vector<std::string> preferred_hca;
    std::vector<std::string> avail_hca;
};

// Partition a GPU's HCAs into preferred vs avail using a 3-level hierarchy:
//   1. Best (lowest) PciPathType wins.
//   2. Fewest hops within the same path type.
//   3. Highest PCIe bandwidth among NICs that tie on the above.
// HCA ancestor chains must be supplied in parallel to all_hca and are
// expected to be pre-computed once by the caller (sysfs lookups are
// expensive).  Insertion order from all_hca is preserved in the output.
inline HcaPartition selectPreferredHcas(
    const std::vector<std::string> &gpu_ancestor_chain, int gpu_numa_node,
    const std::vector<InfinibandDevice> &all_hca,
    const std::vector<std::vector<std::string>> &nic_ancestor_chains) {
    std::unordered_set<std::string> gpu_ancestors(gpu_ancestor_chain.begin(),
                                                  gpu_ancestor_chain.end());

    struct HcaPathInfo {
        const InfinibandDevice *hca;
        PciPathType path_type;
        int hops;
        double pci_bw;
    };

    std::vector<HcaPathInfo> hca_paths;
    hca_paths.reserve(all_hca.size());
    PciPathType best_path_type = PciPathType::DIS;
    int best_hops = INT_MAX;
    double best_bw = 0.0;

    for (size_t j = 0; j < all_hca.size(); ++j) {
        const auto &hca = all_hca[j];
        const auto &nic_ancestor_chain = nic_ancestor_chains[j];
        auto [path_type, hops] =
            classifyGpuNicPath(gpu_ancestor_chain, gpu_ancestors, gpu_numa_node,
                               hca.numa_node, nic_ancestor_chain);
        hca_paths.push_back(HcaPathInfo{.hca = &hca,
                                        .path_type = path_type,
                                        .hops = hops,
                                        .pci_bw = hca.pci_bw_gbps});

        int hop_rank = hops >= 0 ? hops : INT_MAX;
        if (static_cast<int>(path_type) < static_cast<int>(best_path_type) ||
            (path_type == best_path_type && hop_rank < best_hops)) {
            best_path_type = path_type;
            best_hops = hop_rank;
            best_bw = hca.pci_bw_gbps;
        } else if (path_type == best_path_type && hop_rank == best_hops &&
                   hca.pci_bw_gbps > best_bw) {
            best_bw = hca.pci_bw_gbps;
        }
    }

    HcaPartition result;
    for (const auto &path_info : hca_paths) {
        bool is_preferred = path_info.path_type == best_path_type;
        if (is_preferred && best_hops != INT_MAX) {
            is_preferred = path_info.hops == best_hops;
        }
        // Among NICs with same path type and hops, prefer those with the
        // highest PCIe bandwidth.  This filters out lower-bandwidth
        // front-end/management NICs automatically.
        if (is_preferred && best_bw > 0.0) {
            is_preferred = path_info.pci_bw >= best_bw;
        }
        if (is_preferred) {
            result.preferred_hca.push_back(path_info.hca->name);
        } else {
            result.avail_hca.push_back(path_info.hca->name);
        }
    }
    return result;
}

}  // namespace mooncake

#endif  // PCI_TOPOLOGY_H

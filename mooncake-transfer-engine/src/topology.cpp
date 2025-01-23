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

#include <glog/logging.h>
#include <jsoncpp/json/json.h>

#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

#include <ctype.h>
#include <dirent.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "topology.h"

namespace mooncake {
struct InfinibandDevice {
    std::string name;
    std::string pci_bus_id;
    int numa_node;
};

static std::vector<InfinibandDevice> listInfiniBandDevices() {
    DIR *dir = opendir("/sys/class/infiniband");
    struct dirent *entry;
    std::vector<InfinibandDevice> devices;

    if (dir == NULL) {
        PLOG(WARNING) << "Failed to open /sys/class/infiniband";
        return {};
    }
    while ((entry = readdir(dir))) {
        if (entry->d_name[0] == '.') {
            continue;
        }

        std::string device_name = entry->d_name;

        char path[PATH_MAX + 32];
        char resolved_path[PATH_MAX];
        // Get the PCI bus id for the infiniband device. Note that
        // "/sys/class/infiniband/mlx5_X/" is a symlink to
        // "/sys/devices/pciXXXX:XX/XXXX:XX:XX.X/infiniband/mlx5_X/".
        snprintf(path, sizeof(path), "/sys/class/infiniband/%s/../..",
                 entry->d_name);
        if (realpath(path, resolved_path) == NULL) {
            PLOG(ERROR) << "Failed to parse realpath";
            continue;
        }
        std::string pci_bus_id = basename(resolved_path);

        int numa_node = -1;
        snprintf(path, sizeof(path), "%s/numa_node", resolved_path);
        std::ifstream(path) >> numa_node;

        devices.push_back(InfinibandDevice{.name = std::move(device_name),
                                           .pci_bus_id = std::move(pci_bus_id),
                                           .numa_node = numa_node});
    }
    (void)closedir(dir);
    return devices;
}

static std::vector<TopologyEntry> discoverCpuTopology(
    const std::vector<InfinibandDevice> &all_hca) {
    DIR *dir = opendir("/sys/devices/system/node");
    struct dirent *entry;
    std::vector<TopologyEntry> topology;

    if (dir == NULL) {
        PLOG(WARNING) << "Failed to open /sys/devices/system/node";
        return {};
    }
    while ((entry = readdir(dir))) {
        const char *prefix = "node";
        if (entry->d_type != DT_DIR ||
            strncmp(entry->d_name, prefix, strlen(prefix)) != 0) {
            continue;
        }
        int node_id = atoi(entry->d_name + strlen(prefix));
        std::vector<std::string> preferred_hca;
        std::vector<std::string> avail_hca;
        // an HCA connected to the same cpu NUMA node is preferred
        for (const auto &hca : all_hca) {
            if (hca.numa_node == node_id) {
                preferred_hca.push_back(hca.name);
            } else {
                avail_hca.push_back(hca.name);
            }
        }
        topology.push_back(
            TopologyEntry{.name = "cpu:" + std::to_string(node_id),
                          .preferred_hca = std::move(preferred_hca),
                          .avail_hca = std::move(avail_hca)});
    }
    (void)closedir(dir);
    return topology;
}

#ifdef USE_CUDA

static int getPciDistance(const char *bus1, const char *bus2) {
    char buf[PATH_MAX];
    char path1[PATH_MAX];
    char path2[PATH_MAX];
    snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus1);
    if (realpath(buf, path1) == NULL) {
        return -1;
    }
    snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus2);
    if (realpath(buf, path2) == NULL) {
        return -1;
    }

    char *ptr1 = path1;
    char *ptr2 = path2;
    while (*ptr1 && *ptr1 == *ptr2) {
        ptr1++;
        ptr2++;
    }
    int distance = 0;
    for (; *ptr1; ptr1++) {
        distance += (*ptr1 == '/');
    }
    for (; *ptr2; ptr2++) {
        distance += (*ptr2 == '/');
    }

    return distance;
}

static std::vector<TopologyEntry> discoverCudaTopology(
    const std::vector<InfinibandDevice> &all_hca) {
    std::vector<TopologyEntry> topology;
    int device_count;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess) {
        device_count = 0;
    }
    for (int i = 0; i < device_count; i++) {
        char pci_bus_id[20];
        if (cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), i) !=
            cudaSuccess) {
            continue;
        }
        for (char *ch = pci_bus_id; (*ch = tolower(*ch)); ch++);

        std::vector<std::string> preferred_hca;
        std::vector<std::string> avail_hca;
        for (const auto &hca : all_hca) {
            // FIXME: currently we only identify the NICs connected to the same
            // PCIe switch/RC with GPU as preferred.
            if (getPciDistance(hca.pci_bus_id.c_str(), pci_bus_id) == 0) {
                preferred_hca.push_back(hca.name);
            } else {
                avail_hca.push_back(hca.name);
            }
        }
        topology.push_back(
            TopologyEntry{.name = "cuda:" + std::to_string(i),
                          .preferred_hca = std::move(preferred_hca),
                          .avail_hca = std::move(avail_hca)});
    }
    return topology;
}

#endif  // USE_CUDA

Topology::Topology() {}

Topology::~Topology() {}

bool Topology::empty() const { return matrix_.empty(); }

void Topology::clear() {
    matrix_.clear();
    hca_list_.clear();
    resolved_matrix_.clear();
}

int Topology::discover() {
    matrix_.clear();
    auto all_hca = listInfiniBandDevices();
    for (auto &ent : discoverCpuTopology(all_hca)) {
        matrix_[ent.name] = ent;
    }
#ifdef USE_CUDA
    for (auto &ent : discoverCudaTopology(all_hca)) {
        matrix_[ent.name] = ent;
    }
#endif
    return resolve();
}

int Topology::parse(const std::string &topology_json) {
    std::set<std::string> rnic_set;
    Json::Value root;
    Json::Reader reader;

    if (topology_json.empty() || !reader.parse(topology_json, root)) {
        LOG(ERROR) << "Topology: malformed json format";
        return ERR_MALFORMED_JSON;
    }

    matrix_.clear();
    for (const auto &key : root.getMemberNames()) {
        const Json::Value &value = root[key];
        if (value.isArray() && value.size() == 2) {
            TopologyEntry topo_entry;
            topo_entry.name = key;
            for (const auto &array : value[0]) {
                auto device_name = array.asString();
                topo_entry.preferred_hca.push_back(device_name);
            }
            for (const auto &array : value[1]) {
                auto device_name = array.asString();
                topo_entry.avail_hca.push_back(device_name);
            }
            matrix_[key] = topo_entry;
        } else {
            LOG(ERROR) << "Topology: malformed json format";
            return ERR_MALFORMED_JSON;
        }
    }

    return resolve();
}

std::string Topology::toString() const {
    Json::Value value(Json::objectValue);
    for (auto &entry : matrix_) {
        value[entry.first] = entry.second.toJson();
    }
    return value.toStyledString();
}

Json::Value Topology::toJson() const {
    Json::Value root;
    Json::Reader reader;
    reader.parse(toString(), root);
    return root;
}

int Topology::selectDevice(const std::string storage_type, int retry_count) {
    if (resolved_matrix_.count(storage_type) == 0) return ERR_DEVICE_NOT_FOUND;

    auto &entry = resolved_matrix_[storage_type];
    if (retry_count == 0) {
        int rand_value = SimpleRandom::Get().next();
        if (!entry.preferred_hca.empty())
            return entry.preferred_hca[rand_value % entry.preferred_hca.size()];
        else
            return entry.avail_hca[rand_value % entry.avail_hca.size()];
    } else {
        size_t index = (retry_count - 1) %
                       (entry.preferred_hca.size() + entry.avail_hca.size());
        if (index < entry.preferred_hca.size())
            return entry.preferred_hca[index];
        else {
            index -= entry.preferred_hca.size();
            return entry.avail_hca[index];
        }
    }
    return 0;
}

int Topology::resolve() {
    std::map<std::string, int> hca_id_map;
    int next_hca_map_index = 0;
    for (auto &entry : matrix_) {
        for (auto &hca : entry.second.preferred_hca) {
            if (!hca_id_map.count(hca)) {
                hca_list_.push_back(hca);
                hca_id_map[hca] = next_hca_map_index;
                next_hca_map_index++;

                // "*" means any device
                resolved_matrix_["*"].preferred_hca.push_back(hca_id_map[hca]);
            }
            resolved_matrix_[entry.first].preferred_hca.push_back(
                hca_id_map[hca]);
        }
        for (auto &hca : entry.second.avail_hca) {
            if (!hca_id_map.count(hca)) {
                hca_list_.push_back(hca);
                hca_id_map[hca] = next_hca_map_index;
                next_hca_map_index++;

                // "*" means any device
                resolved_matrix_["*"].preferred_hca.push_back(hca_id_map[hca]);
            }
            resolved_matrix_[entry.first].avail_hca.push_back(hca_id_map[hca]);
        }
    }
    return 0;
}
}  // namespace mooncake

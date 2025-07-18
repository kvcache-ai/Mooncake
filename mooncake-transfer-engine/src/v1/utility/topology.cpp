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

#include "v1/utility/topology.h"

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
#include <infiniband/verbs.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "v1/common.h"
#include "v1/utility/memory_location.h"

namespace mooncake {
namespace v1 {
struct InfinibandDevice {
    std::string name;
    std::string pci_bus_id;
    int numa_node;
};

static std::vector<InfinibandDevice> listInfiniBandDevices() {
    int num_devices = 0;
    std::vector<InfinibandDevice> devices;

    struct ibv_device **device_list = ibv_get_device_list(&num_devices);
    if (!device_list || num_devices <= 0) {
        LOG(WARNING) << "No RDMA devices found, check your device installation";
        return {};
    }

    for (int i = 0; i < num_devices; ++i) {
        std::string device_name = ibv_get_device_name(device_list[i]);
        char path[PATH_MAX + 32];
        char resolved_path[PATH_MAX];
        // Get the PCI bus id for the infiniband device. Note that
        // "/sys/class/infiniband/mlx5_X/" is a symlink to
        // "/sys/devices/pciXXXX:XX/XXXX:XX:XX.X/infiniband/mlx5_X/".
        snprintf(path, sizeof(path), "/sys/class/infiniband/%s/../..",
                 device_name.c_str());
        if (realpath(path, resolved_path) == NULL) {
            PLOG(ERROR) << "listInfiniBandDevices: realpath " << path
                        << " failed";
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
    return devices;
}

static void filterInfiniBandDevices(std::vector<InfinibandDevice> &devices,
                                    std::shared_ptr<ConfigManager> conf) {
    auto whitelist = conf->getArray("topology/rdma_whitelist");
    auto blacklist = conf->getArray("topology/rdma_blacklist");
    std::vector<InfinibandDevice> new_devices;
    if (!whitelist.empty()) {
        for (auto &entry : devices) {
            if (std::find(whitelist.begin(), whitelist.end(), entry.name) !=
                whitelist.end())
                new_devices.push_back(entry);
        }
        devices.swap(new_devices);
        return;
    }
    if (!blacklist.empty()) {
        for (auto &entry : devices) {
            if (std::find(blacklist.begin(), blacklist.end(), entry.name) ==
                blacklist.end())
                new_devices.push_back(entry);
        }
        devices.swap(new_devices);
        return;
    }
}

static std::vector<TopologyEntry> discoverCpuTopology(
    const std::vector<InfinibandDevice> &all_hca) {
    DIR *dir = opendir("/sys/devices/system/node");
    struct dirent *entry;
    std::vector<TopologyEntry> topology;

    if (dir == NULL) {
        PLOG(WARNING)
            << "discoverCpuTopology: open /sys/devices/system/node failed";
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
    auto err = cudaGetDeviceCount(&device_count);
    if (err != cudaSuccess) {
        LOG(WARNING) << "cudaGetDeviceCount: " << cudaGetErrorString(err);
        device_count = 0;
    }
    for (int i = 0; i < device_count; i++) {
        char pci_bus_id[20];
        err = cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), i);
        if (err != cudaSuccess) {
            LOG(WARNING) << "cudaDeviceGetPCIBusId: " << cudaGetErrorString(err);
            continue;
        }
        for (char *ch = pci_bus_id; (*ch = tolower(*ch)); ch++);

        std::vector<std::string> preferred_hca;
        std::vector<std::string> avail_hca;

        int min_distance = INT_MAX;
        std::unordered_map<int, std::vector<std::string>> distance_map;
        for (const auto &hca : all_hca) {
            int dist = getPciDistance(hca.pci_bus_id.c_str(), pci_bus_id);
            distance_map[dist].push_back(hca.name);
            min_distance = std::min(min_distance, dist);
        }

        if (distance_map.count(0)) {
            // Prefer NICs with distance 0 (e.g. same PCIe switch/RC)
            preferred_hca = std::move(distance_map[0]);
        } else if (distance_map.count(min_distance)) {
            // No exact match — fall back to NICs with closest PCIe distance
            preferred_hca = std::move(distance_map[min_distance]);
        }

        // Fill avail_hca with remaining NICs not in preferred_hca
        std::unordered_set<std::string> preferred_set(preferred_hca.begin(),
                                                      preferred_hca.end());
        for (const auto &hca : all_hca) {
            if (!preferred_set.count(hca.name)) {
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

Status Topology::discover(std::shared_ptr<ConfigManager> conf) {
    matrix_.clear();
    auto all_hca = listInfiniBandDevices();
    if (conf) filterInfiniBandDevices(all_hca, conf);
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

Status Topology::parse(const std::string &topology_json) {
    std::set<std::string> rnic_set;
    Json::Value root;
    Json::Reader reader;

    if (topology_json.empty() || !reader.parse(topology_json, root)) {
        return Status::MalformedJson(
            "Unrecognized format of topology json" LOC_MARK);
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
            return Status::MalformedJson(
                "Unrecognized format of topology json" LOC_MARK);
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

Status Topology::selectDevice(int &device_id, const std::string storage_type,
                              int retry_count) {
    if (resolved_matrix_.count(storage_type) == 0) {
        auto msg = "No device found in storage type " + storage_type + LOC_MARK;
        return Status::DeviceNotFound(msg);
    }
    auto &entry = resolved_matrix_[storage_type];
    if (retry_count == 0) {
        if (!entry.preferred_hca.empty())
            device_id = entry.preferred_hca[SimpleRandom::Get().next(
                entry.preferred_hca.size())];
        else
            device_id = entry.avail_hca[SimpleRandom::Get().next(
                entry.avail_hca.size())];
    } else {
        size_t index = (retry_count - 1) %
                       (entry.preferred_hca.size() + entry.avail_hca.size());
        if (index < entry.preferred_hca.size())
            device_id = entry.preferred_hca[index];
        else {
            index -= entry.preferred_hca.size();
            device_id = entry.avail_hca[index];
        }
    }
    return Status::OK();
}

Status Topology::resolve() {
    resolved_matrix_.clear();
    hca_list_.clear();
    std::map<std::string, int> hca_id_map;
    int next_hca_map_index = 0;
    for (auto &entry : matrix_) {
        for (auto &hca : entry.second.preferred_hca) {
            if (!hca_id_map.count(hca)) {
                hca_list_.push_back(hca);
                hca_id_map[hca] = next_hca_map_index;
                next_hca_map_index++;

                resolved_matrix_[kWildcardLocation].preferred_hca.push_back(
                    hca_id_map[hca]);
            }
            resolved_matrix_[entry.first].preferred_hca.push_back(
                hca_id_map[hca]);
        }
        for (auto &hca : entry.second.avail_hca) {
            if (!hca_id_map.count(hca)) {
                hca_list_.push_back(hca);
                hca_id_map[hca] = next_hca_map_index;
                next_hca_map_index++;

                resolved_matrix_[kWildcardLocation].preferred_hca.push_back(
                    hca_id_map[hca]);
            }
            resolved_matrix_[entry.first].avail_hca.push_back(hca_id_map[hca]);
        }
    }
    return Status::OK();
}

Status Topology::disableDevice(const std::string &device_name) {
    for (auto &record : matrix_) {
        auto &preferred_hca = record.second.preferred_hca;
        auto preferred_hca_iter =
            std::find(preferred_hca.begin(), preferred_hca.end(), device_name);
        if (preferred_hca_iter != preferred_hca.end())
            preferred_hca.erase(preferred_hca_iter);
        auto &avail_hca = record.second.avail_hca;
        auto avail_hca_iter =
            std::find(avail_hca.begin(), avail_hca.end(), device_name);
        if (avail_hca_iter != avail_hca.end()) avail_hca.erase(avail_hca_iter);
    }
    return resolve();
}
}  // namespace v1
}  // namespace mooncake

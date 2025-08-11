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

#include <unordered_set>

#include "v1/common/status.h"
#include "v1/memory/location.h"
#include "v1/utility/random.h"

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
    const std::vector<InfinibandDevice> &all_devices) {
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
        int numa_node = atoi(entry->d_name + strlen(prefix));

        TopologyEntry entry;
        entry.name = "cpu:" + std::to_string(numa_node);
        entry.numa_node = numa_node;
        // an HCA connected to the same cpu NUMA node is preferred
        for (const auto &device : all_devices) {
            if (device.numa_node == numa_node) {
                entry.device_list[0].push_back(device.name);
            } else {
                entry.device_list[2].push_back(device.name);
            }
        }
        topology.push_back(std::move(entry));
    }
    (void)closedir(dir);
    return topology;
}

static int getNumaNodeFromPciDevice(const std::string &pci_bdf) {
    std::string sysfs_path = "/sys/bus/pci/devices/" + pci_bdf + "/numa_node";
    std::ifstream numa_file(sysfs_path);
    if (!numa_file.is_open()) return -1;
    int numa_node = -1;
    numa_file >> numa_node;
    if (numa_file.fail()) return -1;
    return numa_node;
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
    const std::vector<InfinibandDevice> &all_devices) {
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
            LOG(WARNING) << "cudaDeviceGetPCIBusId: "
                         << cudaGetErrorString(err);
            continue;
        }
        for (char *ch = pci_bus_id; (*ch = tolower(*ch)); ch++);
        int numa_node = getNumaNodeFromPciDevice(pci_bus_id);
        std::vector<std::string> nearest, same_socket, inter_socket;
        int min_distance = INT_MAX;
        std::unordered_map<int, std::vector<std::string>> distance_map;
        for (const auto &device : all_devices) {
            int dist = getPciDistance(device.pci_bus_id.c_str(), pci_bus_id);
            distance_map[dist].push_back(device.name);
            min_distance = std::min(min_distance, dist);
        }

        TopologyEntry entry;
        entry.name = "cuda:" + std::to_string(i);
        entry.numa_node = numa_node;
        if (distance_map.count(0)) {
            // Prefer NICs with distance 0 (e.g. same PCIe switch/RC)
            entry.device_list[0] = std::move(distance_map[0]);
        } else if (distance_map.count(min_distance)) {
            // No exact match — fall back to NICs with closest PCIe distance
            entry.device_list[0] = std::move(distance_map[min_distance]);
        }

        std::unordered_set<std::string> preferred_set(
            entry.device_list[0].begin(), entry.device_list[0].end());
        for (const auto &device : all_devices) {
            if (preferred_set.count(device.name)) continue;
            if (numa_node >= 0 && device.numa_node == numa_node)
                entry.device_list[1].push_back(device.name);
            else
                entry.device_list[2].push_back(device.name);
        }

        topology.push_back(std::move(entry));
    }
    return topology;
}

#endif  // USE_CUDA

Topology::Topology() {}

Topology::~Topology() {}

bool Topology::empty() const { return matrix_.empty(); }

void Topology::clear() {
    matrix_.clear();
    rdma_device_list_.clear();
    resolved_matrix_.clear();
}

Status Topology::discover(std::shared_ptr<ConfigManager> conf) {
    matrix_.clear();
    auto all_device = listInfiniBandDevices();
    if (conf) filterInfiniBandDevices(all_device, conf);
    for (auto &ent : discoverCpuTopology(all_device)) {
        matrix_[ent.name] = ent;
    }
#ifdef USE_CUDA
    for (auto &ent : discoverCudaTopology(all_device)) {
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
        TopologyEntry topo_entry;
        topo_entry.name = key;

        if (value.isArray()) {
            // Old format (just arrays of devices)
            for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
                for (const auto &array : value[(int)rank]) {
                    topo_entry.device_list[rank].push_back(array.asString());
                }
            }
        } else if (value.isObject() && value.isMember("devices")) {
            // New format with numa_node
            if (value.isMember("numa_node") && value["numa_node"].isInt()) {
                topo_entry.numa_node = value["numa_node"].asInt();
            }

            const Json::Value &device_array = value["devices"];
            if (!device_array.isArray()) {
                return Status::MalformedJson(
                    "Expected 'devices' to be an array" LOC_MARK);
            }

            for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
                for (const auto &array : device_array[(int)rank]) {
                    topo_entry.device_list[rank].push_back(array.asString());
                }
            }
        } else {
            return Status::MalformedJson(
                "Unrecognized format of topology json" LOC_MARK);
        }

        matrix_[key] = topo_entry;
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

Status Topology::selectDevice(int &device_id, const std::string &storage_type,
                              int retry_count, int &rand_seed) {
    if (resolved_matrix_.count(storage_type) == 0) {
        auto msg = "No device found in storage type " + storage_type + LOC_MARK;
        return Status::DeviceNotFound(msg);
    }
    auto &entry = resolved_matrix_[storage_type];
    if (retry_count == 0) {
        for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
            auto &list = entry.device_list[rank];
            if (list.empty()) continue;
            if (rand_seed < 0)
                rand_seed = SimpleRandom::Get().next(32);
            device_id = list[rand_seed % list.size()];
            return Status::OK();
        }
    }
    for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
        auto &list = entry.device_list[rank];
        if (list.empty()) continue;
        if (retry_count >= (int)list.size())
            retry_count -= list.size();
        else {
            device_id = list[retry_count];
            return Status::OK();
        }
    }
    device_id = 0;
    return Status::OK();
}

Status Topology::resolve() {
    resolved_matrix_.clear();
    rdma_device_list_.clear();
    std::map<std::string, int> device_id_map;
    int next_device_map_index = 0;
    for (auto &entry : matrix_) {
        for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
            for (auto &device : entry.second.device_list[rank]) {
                if (!device_id_map.count(device)) {
                    rdma_device_list_.push_back(device);
                    device_id_map[device] = next_device_map_index;
                    next_device_map_index++;
                    resolved_matrix_[kWildcardLocation]
                        .device_list[rank]
                        .push_back(device_id_map[device]);
                }
                resolved_matrix_[entry.first].device_list[rank].push_back(
                    device_id_map[device]);
            }
        }
    }
    return Status::OK();
}

Status Topology::disableDevice(const std::string &device_name) {
    for (auto &record : matrix_) {
        for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
            auto &device_list = record.second.device_list[rank];
            auto iter =
                std::find(device_list.begin(), device_list.end(), device_name);
            if (iter != device_list.end()) device_list.erase(iter);
        }
    }
    return resolve();
}
}  // namespace v1
}  // namespace mooncake

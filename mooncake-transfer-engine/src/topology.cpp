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

#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <ctype.h>
#include <dirent.h>
#include <infiniband/verbs.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "cuda_alike.h"
#include "memory_location.h"
#include "topology.h"

namespace mooncake {

struct InfinibandDevice {
    std::string name;
    std::string pci_bus_id;
    int numa_node;
};

static std::vector<InfinibandDevice> listInfiniBandDevices(
    const std::vector<std::string> &filter) {
    int num_devices = 0;
    std::vector<InfinibandDevice> devices;

    struct ibv_device **device_list = ibv_get_device_list(&num_devices);
    if (!device_list) {
        LOG(WARNING) << "No RDMA devices found, check your device installation";
        return {};
    }
    if (device_list && num_devices <= 0) {
        LOG(WARNING) << "No RDMA devices found, check your device installation";
        ibv_free_device_list(device_list);
        return {};
    }

    for (int i = 0; i < num_devices; ++i) {
        std::string device_name = ibv_get_device_name(device_list[i]);
        if (!filter.empty() && std::find(filter.begin(), filter.end(),
                                         device_name) == filter.end())
            continue;
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
    ibv_free_device_list(device_list);
    return devices;
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

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)

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

static bool isSameNumaNode(const char *bus1, const char *bus2) {
    char path[PATH_MAX];
    int numa1 = -1;
    int numa2 = -1;
    snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/numa_node", bus1);
    std::ifstream(path) >> numa1;
    snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/numa_node", bus2);
    std::ifstream(path) >> numa2;
    return (numa1 != -1 && numa1 == numa2);
}

static size_t getCommonParentLength(const char *path1, const char *path2) {
    if (!path1 || !path2) {
        return 0;
    }

    size_t offset = 0;
    size_t parent_length = 0;

    do {
        if (((path1[offset] == '/') || (path1[offset] == '\0')) &&
            ((path2[offset] == '/') || (path2[offset] == '\0'))) {
            parent_length = offset;
        }
    } while ((path1[offset] == path2[offset]) && (path1[offset++] != '\0'));

    return parent_length;
}

static std::string getCommonParent(const char *path1, const char *path2) {
    size_t parent_length = getCommonParentLength(path1, path2);
    eturn std::string(path1, parent_length);
}

static bool isPciRootComplex(const char *path) {
    if (!path) {
        return false;
    }

    int count = -1;
    sscanf(path, "/sys/devices/pci%*x:%*x%n", &count);
    return count == strlen(path);
}

static bool isSamePcieRootComplex(const char *bus1, const char *bus2) {
    if (!bus1 || !bus2) {
        return false;
    }

    char buf[PATH_MAX];
    char resolved1[PATH_MAX];
    char resolved2[PATH_MAX];

    snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus1);
    if (realpath(buf, resolved1) == NULL) {
        return false;
    }

    snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus2);
    if (realpath(buf, resolved2) == NULL) {
        return false;
    }

    std::string common = getCommonParent(resolved1, resolved2);
    if (common.empty()) {
        return false;
    }

    while (!common.empty()) {
        if (isPciRootComplex(common.c_str())) {
            return true;
        }

        auto pos = common.find_last_of('/');
        if (pos == std::string::npos) {
            break;
        }
        common.resize(pos);
    }

    return false;
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

        // Find HCAs with minimum distance in one pass
        int min_distance = INT_MAX;
        std::vector<std::string> min_distance_hcas;

        std::vector<InfinibandDevice> samePCIeRoot_hca;
        std::vector<InfinibandDevice> sameNuma_hca;
        for (const auto &hca : all_hca) {
            if (isSamePcieRootComplex(hca.pci_bus_id.c_str(), pci_bus_id)) {
                same_root_hca.push_back(hca);
            } else if (isSameNumaNode(hca.pci_bus_id.c_str(), pci_bus_id)) {
                sameNuma_hca.push_back(hca);
            }
        }
        const auto &candidate_preferred_hca = 
            !samePCIeRoot_hca.empty() ? samePCIeRoot_hca : 
            !sameNuma_hca.empty() ? sameNuma_hca : 
            all_hca;

        for (const auto &hca : candidate_preferred_hca) {
            int distance = getPciDistance(hca.pci_bus_id.c_str(), pci_bus_id);
            if (distance >= 0) {
                if (distance < min_distance) {
                    min_distance = distance;
                    min_distance_hcas.clear();
                    min_distance_hcas.push_back(hca.name);
                } else if (distance == min_distance) {
                    min_distance_hcas.push_back(hca.name);
                }
            }
        }

        // Add HCAs with minimum distance to preferred_hca, others to avail_hca
        for (const auto &hca : all_hca) {
            if (std::find(min_distance_hcas.begin(), min_distance_hcas.end(),
                          hca.name) != min_distance_hcas.end()) {
                preferred_hca.push_back(hca.name);
            } else {
                avail_hca.push_back(hca.name);
            }
        }
        topology.push_back(
            TopologyEntry{.name = GPU_PREFIX + std::to_string(i),
                          .preferred_hca = std::move(preferred_hca),
                          .avail_hca = std::move(avail_hca)});
    }
    return topology;
}

#endif  // USE_CUDA

Topology::Topology() {
    auto str = getenv("MC_PATH_ROUNDROBIN");
    if (str && (strcmp(str, "1") == 0 || strcasecmp(str, "true") == 0)) {
        use_round_robin_ = true;
    } else {
        use_round_robin_ = false;
    }
}

Topology::~Topology() {}

bool Topology::empty() const {
    for (const auto &entry : resolved_matrix_) {
        if (!entry.second.preferred_hca.empty() ||
            !entry.second.avail_hca.empty()) {
            return false;
        }
    }
    return true;
}

void Topology::clear() {
    matrix_.clear();
    hca_list_.clear();
    resolved_matrix_.clear();
}

int Topology::discover(const std::vector<std::string> &filter) {
    matrix_.clear();
    auto all_hca = listInfiniBandDevices(filter);
    for (auto &ent : discoverCpuTopology(all_hca)) {
        matrix_[ent.name] = ent;
    }
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
    for (auto &ent : discoverCudaTopology(all_hca)) {
        matrix_[ent.name] = ent;
    }
#endif
    return resolve();
}

int Topology::parse(const std::string &topology_json) {
    std::set<std::string> rnic_set;
    Json::Value root;

    if (topology_json.empty()) {
        return ERR_MALFORMED_JSON;
    }

    // Use thread-safe CharReaderBuilder instead of deprecated Reader
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;

    if (!reader->parse(topology_json.data(),
                       topology_json.data() + topology_json.size(), &root,
                       &errs)) {
        LOG(ERROR) << "Topology::parse: JSON parse error: " << errs;
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
    for (const auto &pair : matrix_) {
        root[pair.first] = pair.second.toJson();
    }
    return root;
}

int Topology::selectDevice(const std::string storage_type,
                           std::string_view hint, int retry_count) {
    const auto it = resolved_matrix_.find(std::string(storage_type));
    if (it == resolved_matrix_.end()) {
        return ERR_DEVICE_NOT_FOUND;
    }

    const auto &entry = it->second;

    if (!hint.empty()) {
        auto hca_idx = entry.getHcaIndex(std::string(hint));
        if (hca_idx != -1) {
            return hca_idx;
        }
    }

    return selectDevice(storage_type, retry_count);
}

int Topology::selectDevice(const std::string storage_type, int retry_count) {
    if (resolved_matrix_.count(storage_type) == 0) return ERR_DEVICE_NOT_FOUND;

    auto &entry = resolved_matrix_[storage_type];
    if (retry_count == 0) {
        int rand_value;
        if (use_round_robin_) {
            thread_local int tl_counter = 0;
            rand_value = tl_counter;
            tl_counter = (tl_counter + 1) % 10000;
        } else
            rand_value = SimpleRandom::Get().next();
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
    resolved_matrix_.clear();
    hca_list_.clear();
    std::map<std::string, int> hca_id_map;
    int next_hca_map_index = 0;
    for (auto &entry : matrix_) {
        for (auto &hca : entry.second.preferred_hca) {
            if (!hca_id_map.count(hca)) {
                hca_list_.push_back(hca);
                hca_id_map[hca] = next_hca_map_index++;

                resolved_matrix_[kWildcardLocation].preferred_hca.push_back(
                    hca_id_map[hca]);
                resolved_matrix_[kWildcardLocation]
                    .preferred_hca_name_to_index_map_[hca] = hca_id_map[hca];
            }
            resolved_matrix_[entry.first].preferred_hca.push_back(
                hca_id_map[hca]);
            resolved_matrix_[entry.first]
                .preferred_hca_name_to_index_map_[hca] = hca_id_map[hca];
        }
        for (auto &hca : entry.second.avail_hca) {
            if (!hca_id_map.count(hca)) {
                hca_list_.push_back(hca);
                hca_id_map[hca] = next_hca_map_index++;

                resolved_matrix_[kWildcardLocation].preferred_hca.push_back(
                    hca_id_map[hca]);
                resolved_matrix_[kWildcardLocation]
                    .preferred_hca_name_to_index_map_[hca] = hca_id_map[hca];
            }
            resolved_matrix_[entry.first].avail_hca.push_back(hca_id_map[hca]);
            resolved_matrix_[entry.first].avail_hca_name_to_index_map_[hca] =
                hca_id_map[hca];
        }
    }
    return 0;
}

int Topology::disableDevice(const std::string &device_name) {
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
}  // namespace mooncake

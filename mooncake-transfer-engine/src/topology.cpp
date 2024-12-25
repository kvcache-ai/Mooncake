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

struct InfinibandDevice {
    std::string name;
    std::string pci_bus_id;
    int numa_node;
};

struct TopologyEntry {
    std::string name;
    std::vector<std::string> preferred_hca;
    std::vector<std::string> avail_hca;

    Json::Value to_json() {
        Json::Value matrix(Json::arrayValue);
        Json::Value hca_list(Json::arrayValue);
        for (auto &hca : preferred_hca) {
            hca_list.append(hca);
        }
        matrix.append(hca_list);
        hca_list.clear();
        for (auto &hca : avail_hca) {
            hca_list.append(hca);
        }
        matrix.append(hca_list);
        return matrix;
    }
};

static std::vector<InfinibandDevice> list_infiniband_devices() {
    DIR *dir = opendir("/sys/class/infiniband");
    struct dirent *entry;
    std::vector<InfinibandDevice> devices;

    if (dir == NULL) {
        LOG(WARNING) << "failed to list /sys/class/infiniband";
        return {};
    }
    while ((entry = readdir(dir))) {
        if (entry->d_name[0] == '.') {
            continue;
        }

        std::string device_name = entry->d_name;

        char path[PATH_MAX];
        char resolved_path[PATH_MAX];
        // Get the PCI bus id for the infiniband device. Note that
        // "/sys/class/infiniband/mlx5_X/" is a symlink to
        // "/sys/devices/pciXXXX:XX/XXXX:XX:XX.X/infiniband/mlx5_X/".
        snprintf(path, sizeof(path), "/sys/class/infiniband/%s/../..",
                 entry->d_name);
        if (realpath(path, resolved_path) == NULL) {
            LOG(ERROR) << "realpath: " << strerror(errno);
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

static std::vector<TopologyEntry> discover_cpu_topology(
    const std::vector<InfinibandDevice> &all_hca) {
    DIR *dir = opendir("/sys/devices/system/node");
    struct dirent *entry;
    std::vector<TopologyEntry> topology;

    if (dir == NULL) {
        LOG(WARNING) << "failed to list /sys/devices/system/node";
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

static int get_pci_distance(const char *bus1, const char *bus2) {
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

static std::vector<TopologyEntry> discover_cuda_topology(
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
            if (get_pci_distance(hca.pci_bus_id.c_str(), pci_bus_id) == 0) {
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

namespace mooncake {
// TODO: add black/white lists for devices.
std::string discoverTopologyMatrix() {
    auto all_hca = list_infiniband_devices();
    Json::Value value(Json::objectValue);
    for (auto &ent : discover_cpu_topology(all_hca)) {
        value[ent.name] = ent.to_json();
    }
#ifdef USE_CUDA
    for (auto &ent : discover_cuda_topology(all_hca)) {
        value[ent.name] = ent.to_json();
    }
#endif
    return value.toStyledString();
}

int parseNicPriorityMatrix(const std::string &nic_priority_matrix,
                           TransferMetadata::PriorityMatrix &priority_map,
                           std::vector<std::string> &rnic_list) {
    std::set<std::string> rnic_set;
    Json::Value root;
    Json::Reader reader;

    if (nic_priority_matrix.empty() ||
        !reader.parse(nic_priority_matrix, root)) {
        LOG(ERROR) << "malformed format of NIC priority matrix";
        return ERR_MALFORMED_JSON;
    }

    if (!root.isObject()) {
        LOG(ERROR) << "malformed format of NIC priority matrix";
        return ERR_MALFORMED_JSON;
    }

    priority_map.clear();
    for (const auto &key : root.getMemberNames()) {
        const Json::Value &value = root[key];
        if (value.isArray() && value.size() == 2) {
            TransferMetadata::PriorityItem item;
            for (const auto &array : value[0]) {
                auto device_name = array.asString();
                item.preferred_rnic_list.push_back(device_name);
                auto iter = rnic_set.find(device_name);
                if (iter == rnic_set.end()) {
                    item.preferred_rnic_id_list.push_back(rnic_set.size());
                    rnic_set.insert(device_name);
                } else {
                    item.preferred_rnic_id_list.push_back(
                        std::distance(rnic_set.begin(), iter));
                }
            }
            for (const auto &array : value[1]) {
                auto device_name = array.asString();
                item.available_rnic_list.push_back(device_name);
                auto iter = rnic_set.find(device_name);
                if (iter == rnic_set.end()) {
                    item.available_rnic_id_list.push_back(rnic_set.size());
                    rnic_set.insert(device_name);
                } else {
                    item.available_rnic_id_list.push_back(
                        std::distance(rnic_set.begin(), iter));
                }
            }
            priority_map[key] = item;
        } else {
            LOG(ERROR) << "malformed format of NIC priority matrix";
            return ERR_MALFORMED_JSON;
        }
    }

    rnic_list.clear();
    for (auto &entry : rnic_set) rnic_list.push_back(entry);

    return 0;
}
}  // namespace mooncake

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

#include <cctype>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "pci_topology.h"
#include <dirent.h>
#include <infiniband/verbs.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "cuda_alike.h"
#include "memory_location.h"
#include "topology.h"
#ifdef USE_UB
#include <libgen.h>
#include <urma_api.h>
#endif

namespace mooncake {

static bool isIbDeviceAccessible(struct ibv_device *device) {
    char device_path[PATH_MAX];
    struct stat st;

    snprintf(device_path, sizeof(device_path), "/dev/infiniband/%s",
             device->dev_name);

    if (stat(device_path, &st) != 0) {
        LOG(WARNING) << "Device " << ibv_get_device_name(device) << " path "
                     << device_path << " does not exist";
        return false;
    }

    if (!S_ISCHR(st.st_mode)) {
        LOG(WARNING) << "Device path " << device_path
                     << " is not a character device (mode: " << std::oct
                     << st.st_mode << std::dec << ")";
        return false;
    }

    if (access(device_path, R_OK | W_OK) != 0) {
        LOG(WARNING) << "Device " << device_path
                     << " is not accessible for read/write";
        return false;
    }

    return true;
}

static bool checkIbDevicePort(struct ibv_context *context,
                              const std::string &device_name,
                              uint8_t port_num) {
    struct ibv_port_attr port_attr;

    if (ibv_query_port(context, port_num, &port_attr) != 0) {
        PLOG(WARNING) << "Failed to query port " << static_cast<int>(port_num)
                      << " on " << device_name;
        return false;
    }

    if (port_attr.gid_tbl_len == 0) {
        LOG(WARNING) << device_name << ":" << static_cast<int>(port_num)
                     << " has no GID table entries";
        return false;
    }

    if (port_attr.state != IBV_PORT_ACTIVE) {
        LOG(INFO) << device_name << ":" << static_cast<int>(port_num)
                  << " is not active (state: "
                  << static_cast<int>(port_attr.state) << ")";
        return false;
    }

    return true;
}

static bool isIbDeviceAvailable(struct ibv_device *device) {
    const char *device_name = ibv_get_device_name(device);

    if (!isIbDeviceAccessible(device)) {
        return false;
    }

    struct ibv_context *context = ibv_open_device(device);
    if (!context) {
        PLOG(WARNING) << "Failed to open device " << device_name;
        return false;
    }

    struct ibv_device_attr device_attr;
    if (ibv_query_device(context, &device_attr) != 0) {
        PLOG(WARNING) << "Failed to query device attributes for "
                      << device_name;
        ibv_close_device(context);
        return false;
    }

    bool has_active_port = false;
    for (uint8_t port = 1; port <= device_attr.phys_port_cnt; ++port) {
        if (checkIbDevicePort(context, device_name, port)) {
            has_active_port = true;
            LOG(INFO) << "Device " << device_name << " port "
                      << static_cast<int>(port) << " is available";
        }
    }

    ibv_close_device(context);

    if (!has_active_port) {
        LOG(WARNING) << "Device " << device_name
                     << " has no active ports, skipping";
    }

    return has_active_port;
}

// InfinibandDevice is defined in pci_topology.h

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

        // Check device availability before adding to the list
        if (!isIbDeviceAvailable(device_list[i])) {
            LOG(WARNING) << "Skipping unavailable device: " << device_name;
            continue;
        }

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

        // Read PCIe link speed and width to compute bandwidth.
        // Used to distinguish high-bandwidth backend NICs from
        // lower-bandwidth front-end/management NICs.
        double pci_bw_gbps = 0.0;
        {
            double speed_gts = 0.0;
            int width = 0;
            snprintf(path, sizeof(path), "%s/current_link_speed",
                     resolved_path);
            std::ifstream speed_file(path);
            if (speed_file.is_open()) speed_file >> speed_gts;
            snprintf(path, sizeof(path), "%s/current_link_width",
                     resolved_path);
            std::ifstream width_file(path);
            if (width_file.is_open()) width_file >> width;
            pci_bw_gbps = speed_gts * width;
        }

        devices.push_back(InfinibandDevice{.name = std::move(device_name),
                                           .pci_bus_id = std::move(pci_bus_id),
                                           .numa_node = numa_node,
                                           .pci_bw_gbps = pci_bw_gbps});
    }
    ibv_free_device_list(device_list);
    return devices;
}

#ifdef USE_UB
struct UBDevice {
    std::string name;
    std::string pci_bus_id;
    int numa_node;
};

static std::vector<UBDevice> listUBDevices(
    const std::vector<std::string> &filter) {
    int num_devices = 0;
    std::vector<UBDevice> devices;

    urma_init_attr_t init_attr = {};
    if (urma_init(&init_attr) != URMA_SUCCESS) {
        LOG(WARNING) << "Failed to urma init";
        return {};
    }
    LOG(INFO) << "URMA module init success";
    urma_device_t **device_list = urma_get_device_list(&num_devices);
    if (!device_list) {
        LOG(WARNING) << "No UB devices found, check your device installation";
        urma_uninit();
        return {};
    }
    if (device_list && num_devices <= 0) {
        LOG(WARNING) << "No UB devices found, check your device installation";
        urma_free_device_list(device_list);
        return {};
    }

    for (int i = 0; i < num_devices; ++i) {
        std::string device_name = device_list[i]->name;
        if (!filter.empty() && std::find(filter.begin(), filter.end(),
                                         device_name) == filter.end())
            continue;
        char path[PATH_MAX + 32];
        char resolved_path[PATH_MAX];
        // Get the PCI bus id for the infiniband device. Note that
        snprintf(path, sizeof(path), "/sys/class/ubcore/%s",
                 device_list[i]->name);
        LOG(INFO) << "listUBDevices: path " << path;
        if (realpath(path, resolved_path) == NULL) {
            LOG(ERROR) << "listUBDevices: realpath " << resolved_path
                       << " failed";
            continue;
        }
        LOG(INFO) << "listUBDevices: realpath " << resolved_path;
        std::string pci_bus_id = basename(resolved_path);

        int numa_node = -1;
        snprintf(path, sizeof(path), "%s/numa",
                 dirname(dirname(resolved_path)));
        LOG(INFO) << "listUBDevices: numanodepath " << path;
        std::ifstream(path) >> numa_node;
        LOG(INFO) << "UBDevices : performation node ----"
                  << device_list[i]->name << " : " << numa_node;

        devices.push_back(UBDevice{.name = std::move(device_name),
                                   .pci_bus_id = std::move(pci_bus_id),
                                   .numa_node = numa_node});
    }
    urma_free_device_list(device_list);
    urma_uninit();
    return devices;
}

static std::vector<TopologyEntry> discoverCpuTopology(
    const std::vector<UBDevice> &all_hca) {
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
#endif

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
        // An HCA connected to the same cpu NUMA node is preferred.
        // Among same-NUMA HCAs, only keep those with the highest PCIe
        // bandwidth to filter out lower-bandwidth management NICs.
        double best_bw = 0.0;
        for (const auto &hca : all_hca) {
            if (hca.numa_node == node_id && hca.pci_bw_gbps > best_bw) {
                best_bw = hca.pci_bw_gbps;
            }
        }
        for (const auto &hca : all_hca) {
            if (hca.numa_node == node_id &&
                (best_bw <= 0.0 || hca.pci_bw_gbps >= best_bw)) {
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

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)

// PciPathType, InfinibandDevice, kMaxPciDepth, normalizePciBusId,
// classifyGpuNicPath, and selectPreferredHcas are defined in pci_topology.h.
using mooncake::InfinibandDevice;
using mooncake::normalizePciBusId;
using mooncake::selectPreferredHcas;

static std::string getPciParent(const std::string &normalized_pci_bus_id) {
    std::string path = "/sys/bus/pci/devices/" + normalized_pci_bus_id + "/..";
    char resolved[PATH_MAX];
    if (realpath(path.c_str(), resolved) == nullptr) {
        return "";
    }

    std::string full_path(resolved);
    auto pos = full_path.rfind('/');
    if (pos == std::string::npos) {
        return "";
    }

    std::string parent = full_path.substr(pos + 1);
    if (parent.find(':') != std::string::npos &&
        std::isxdigit(static_cast<unsigned char>(parent[0]))) {
        return parent;
    }
    // Include the PCI root bus (e.g. "pci0008:00") so that devices behind
    // different root ports on the same root complex share a common ancestor.
    if (parent.rfind("pci", 0) == 0 && parent.find(':') != std::string::npos) {
        return parent;
    }
    return "";
}

static std::vector<std::string> buildPciAncestorChain(
    const std::string &pci_bus_id) {
    std::vector<std::string> chain;
    std::string current = normalizePciBusId(pci_bus_id);
    while (!current.empty() && chain.size() < kMaxPciDepth) {
        chain.push_back(current);
        current = getPciParent(current);
    }
    return chain;
}

static int getPciNumaNode(const std::string &pci_bus_id) {
    std::string path =
        "/sys/bus/pci/devices/" + normalizePciBusId(pci_bus_id) + "/numa_node";
    std::ifstream file(path);
    if (!file.is_open()) {
        return -1;
    }

    int numa_node = -1;
    file >> numa_node;
    return numa_node;
}

static std::vector<TopologyEntry> discoverCudaTopology(
    const std::vector<InfinibandDevice> &all_hca) {
    std::vector<TopologyEntry> topology;
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess) {
        device_count = 0;
    }

    // sysfs lookups are expensive; build each HCA's ancestor chain once.
    std::vector<std::vector<std::string>> nic_ancestor_chains;
    nic_ancestor_chains.reserve(all_hca.size());
    for (const auto &hca : all_hca) {
        nic_ancestor_chains.push_back(buildPciAncestorChain(hca.pci_bus_id));
    }

    for (int i = 0; i < device_count; i++) {
        char pci_bus_id[20];
        if (cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), i) !=
            cudaSuccess) {
            continue;
        }

        std::string gpu_pci_bus_id = normalizePciBusId(pci_bus_id);
        int gpu_numa_node = getPciNumaNode(gpu_pci_bus_id);
        auto gpu_ancestor_chain = buildPciAncestorChain(gpu_pci_bus_id);

        auto partition = selectPreferredHcas(gpu_ancestor_chain, gpu_numa_node,
                                             all_hca, nic_ancestor_chains);

        topology.push_back(
            TopologyEntry{.name = GPU_PREFIX + std::to_string(i),
                          .preferred_hca = std::move(partition.preferred_hca),
                          .avail_hca = std::move(partition.avail_hca)});
    }
    return topology;
}
#endif

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
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
    for (auto &ent : discoverCudaTopology(all_hca)) {
        matrix_[ent.name] = ent;
    }
#endif
#ifdef USE_UB
    auto ub_all_hca = listUBDevices(filter);
    for (auto &ent : discoverCpuTopology(ub_all_hca)) {
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

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

#include "tent/platform/cuda.h"
#include "tent/common/status.h"
#include "tent/common/utils/prefault.h"
#include "tent/common/utils/random.h"

#include <glog/logging.h>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <cuda_runtime.h>
#include <ctype.h>
#include <dirent.h>
#include <infiniband/verbs.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unordered_set>

#include <algorithm>

namespace mooncake {
namespace tent {
static std::vector<Topology::NicEntry> listInfiniBandDevices() {
    int num_devices = 0;
    std::vector<Topology::NicEntry> devices;

    struct ibv_device** device_list = ibv_get_device_list(&num_devices);
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
            PLOG(ERROR) << "realpath " << path << ":";
            continue;
        }
        std::string pci_bus_id = basename(resolved_path);

        int numa_node = -1;
        snprintf(path, sizeof(path), "%s/numa_node", resolved_path);
        std::ifstream(path) >> numa_node;

        devices.push_back(
            Topology::NicEntry{.name = std::move(device_name),
                               .pci_bus_id = std::move(pci_bus_id),
                               .type = Topology::NIC_RDMA,
                               .numa_node = numa_node});
    }
    return devices;
}

static void filterInfiniBandDevices(std::vector<Topology::NicEntry>& devices,
                                    std::shared_ptr<Config> conf) {
    auto whitelist = conf->getArray<std::string>("topology/rdma_whitelist");
    auto blacklist = conf->getArray<std::string>("topology/rdma_blacklist");
    std::vector<Topology::NicEntry> new_devices;
    if (!whitelist.empty()) {
        for (auto& entry : devices) {
            if (std::find(whitelist.begin(), whitelist.end(), entry.name) !=
                whitelist.end())
                new_devices.push_back(entry);
        }
        devices.swap(new_devices);
        return;
    }
    if (!blacklist.empty()) {
        for (auto& entry : devices) {
            if (std::find(blacklist.begin(), blacklist.end(), entry.name) ==
                blacklist.end())
                new_devices.push_back(entry);
        }
        devices.swap(new_devices);
        return;
    }
}

static void discoverCpuTopology(std::vector<Topology::NicEntry>& nic_list,
                                std::vector<Topology::MemEntry>& mem_list) {
    DIR* dir = opendir("/sys/devices/system/node");
    struct dirent* entry;
    if (dir == NULL) {
        PLOG(WARNING) << "open /sys/devices/system/node failed";
        return;
    }
    while ((entry = readdir(dir))) {
        const char* prefix = "node";
        if (entry->d_type != DT_DIR ||
            strncmp(entry->d_name, prefix, strlen(prefix)) != 0) {
            continue;
        }
        int numa_node = atoi(entry->d_name + strlen(prefix));
        Topology::MemEntry entry;
        entry.name = "cpu:" + std::to_string(numa_node);
        entry.numa_node = numa_node;
        entry.type = Topology::MEM_HOST;
        int nic_id = 0;
        for (const auto& device : nic_list) {
            if (device.numa_node == numa_node) {
                entry.device_list[0].push_back(nic_id++);
            } else {
                entry.device_list[2].push_back(nic_id++);
            }
        }
        mem_list.push_back(std::move(entry));
    }
    (void)closedir(dir);
    return;
}

static int getNumaNodeFromPciDevice(const std::string& pci_bdf) {
    std::string sysfs_path = "/sys/bus/pci/devices/" + pci_bdf + "/numa_node";
    std::ifstream numa_file(sysfs_path);
    if (!numa_file.is_open()) return -1;
    int numa_node = -1;
    numa_file >> numa_node;
    if (numa_file.fail()) return -1;
    return numa_node;
}

static int getPciDistance(const char* bus1, const char* bus2) {
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

    char* ptr1 = path1;
    char* ptr2 = path2;
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

static void discoverCudaTopology(std::vector<Topology::NicEntry>& nic_list,
                                 std::vector<Topology::MemEntry>& mem_list) {
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
        for (char* ch = pci_bus_id; (*ch = tolower(*ch)); ch++);
        int numa_node = getNumaNodeFromPciDevice(pci_bus_id);
        int min_distance = INT_MAX;
        std::unordered_map<int, std::vector<int>> distance_map;
        for (const auto& device : nic_list) {
            int dist = getPciDistance(device.pci_bus_id.c_str(), pci_bus_id);
            distance_map[dist].push_back(&device - &nic_list[0]);
            min_distance = std::min(min_distance, dist);
        }

        Topology::MemEntry entry;
        entry.name = "cuda:" + std::to_string(i);
        entry.numa_node = numa_node;
        entry.pci_bus_id = pci_bus_id;
        entry.type = Topology::MEM_CUDA;
        if (distance_map.count(0)) {
            // Prefer NICs with distance 0 (e.g. same PCIe switch/RC)
            entry.device_list[0] = std::move(distance_map[0]);
        } else if (distance_map.count(min_distance)) {
            // No exact match — fall back to NICs with closest PCIe distance
            entry.device_list[0] = std::move(distance_map[min_distance]);
        }
        std::unordered_set<int> preferred_set;
        for (const auto& dev_id : entry.device_list[0]) {
            preferred_set.insert(dev_id);
        }
        int dev_id = 0;
        for (const auto& device : nic_list) {
            if (!preferred_set.count(dev_id)) {
                if (numa_node >= 0 && device.numa_node == numa_node)
                    entry.device_list[1].push_back(dev_id);
                else
                    entry.device_list[2].push_back(dev_id);
            }
            dev_id++;
        }
        mem_list.push_back(std::move(entry));
    }
    return;
}

static void insertFallbackMemEntry(int nic_list_count,
                                   std::vector<Topology::MemEntry>& mem_list) {
    for (auto& entry : mem_list) {
        if (entry.name == kWildcardLocation) {
            entry.device_list[2].clear();
            for (int i = 0; i < nic_list_count; ++i)
                entry.device_list[2].push_back(i);
            return;
        }
    }
    Topology::MemEntry new_entry;
    new_entry.name = kWildcardLocation;
    new_entry.numa_node = -1;
    new_entry.type = Topology::MEM_HOST;
    for (int i = 0; i < nic_list_count; ++i)
        new_entry.device_list[2].push_back(i);
    mem_list.push_back(new_entry);
}

Status CudaPlatform::probe(std::vector<Topology::NicEntry>& nic_list,
                           std::vector<Topology::MemEntry>& mem_list) {
    auto new_nic_list = listInfiniBandDevices();
    filterInfiniBandDevices(new_nic_list, conf);
    for (auto& entry : new_nic_list) nic_list.push_back(entry);
    insertFallbackMemEntry((int)nic_list.size(), mem_list);
    discoverCpuTopology(nic_list, mem_list);
    discoverCudaTopology(nic_list, mem_list);
    return Status::OK();
}

MemoryType CudaPlatform::getMemoryType(void* addr) {
    cudaPointerAttributes attributes;
    cudaError_t result;
    result = cudaPointerGetAttributes(&attributes, addr);
    if (result != cudaSuccess) {
        LOG(WARNING) << "cudaPointerGetAttributes: "
                     << cudaGetErrorString(result);
        return MTYPE_UNKNOWN;
    }
    if (attributes.type == cudaMemoryTypeDevice) return MTYPE_CUDA;
    return MTYPE_CPU;
}

static inline uintptr_t alignPage(uintptr_t address) {
    const static size_t kPageSize = 4096;
    return address & ~(kPageSize - 1);
}

static inline std::string genCpuNodeName(int node) {
    if (node >= 0) return "cpu:" + std::to_string(node);
    return kWildcardLocation;
}

static inline std::string genCudaNodeName(int node) {
    if (node >= 0) return "cuda:" + std::to_string(node);
    return kWildcardLocation;
}

const std::vector<RangeLocation> CudaPlatform::getLocation(void* start,
                                                           size_t len,
                                                           bool skip_prefault) {
    const static size_t kPageSize = 4096;
    std::vector<RangeLocation> entries;

    cudaPointerAttributes attributes;
    cudaError_t result;

    result = cudaPointerGetAttributes(&attributes, start);
    if (result != cudaSuccess) {
        LOG(WARNING) << "cudaPointerGetAttributes: "
                     << cudaGetErrorString(result);
        entries.push_back({(uint64_t)start, len, kWildcardLocation});
        return entries;
    }

    if (attributes.type == cudaMemoryTypeDevice) {
        entries.push_back(
            {(uint64_t)start, len, genCudaNodeName(attributes.device)});
        return entries;
    }

    // start and end address may not be page aligned.
    uintptr_t aligned_start = alignPage((uintptr_t)start);
    int n =
        (uintptr_t(start) - aligned_start + len + kPageSize - 1) / kPageSize;
    void** pages = (void**)malloc(sizeof(void*) * n);
    int* status = (int*)malloc(sizeof(int) * n);

    for (int i = 0; i < n; i++) {
        pages[i] = (void*)((char*)aligned_start + i * kPageSize);
    }

    if (!skip_prefault) {
        prefaultBeforeProbe(pages, n, aligned_start, "CudaPlatform");
    }

    int rc = numa_move_pages(0, n, pages, nullptr, status, 0);
    if (rc != 0) {
        // PLOG(WARNING) << "Failed to get NUMA node, addr: " << start
        //               << ", len: " << len;
        entries.push_back({(uint64_t)start, len, kWildcardLocation});
        ::free(pages);
        ::free(status);
        return entries;
    }

    int node = status[0];
    uint64_t start_addr = (uint64_t)start;
    uint64_t new_start_addr;
    for (int i = 1; i < n; i++) {
        if (status[i] != node) {
            new_start_addr = alignPage((uint64_t)start) + i * kPageSize;
            entries.push_back({start_addr, size_t(new_start_addr - start_addr),
                               genCpuNodeName(node)});
            start_addr = new_start_addr;
            node = status[i];
        }
    }
    entries.push_back(
        {start_addr, (uint64_t)start + len - start_addr, genCpuNodeName(node)});
    ::free(pages);
    ::free(status);
    return entries;
}

}  // namespace tent
}  // namespace mooncake

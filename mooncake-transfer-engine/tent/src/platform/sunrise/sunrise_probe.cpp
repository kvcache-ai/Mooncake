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

#include "tent/platform/sunrise.h"

#include <algorithm>
#include <dirent.h>
#include <fstream>
#include <infiniband/verbs.h>
#include <limits.h>
#include <numa.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include "tent/common/utils/prefault.h"
#include <glog/logging.h>
#include <tang_runtime_api.h>

namespace mooncake {
namespace tent {

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

static bool isIbDeviceAccessible(struct ibv_device* device) {
    char device_path[PATH_MAX];
    struct stat st;

    snprintf(device_path, sizeof(device_path), "/dev/infiniband/%s",
             device->dev_name);
    if (stat(device_path, &st) != 0) return false;
    if (!S_ISCHR(st.st_mode)) return false;
    if (access(device_path, R_OK | W_OK) != 0) return false;
    return true;
}

static bool checkIbDevicePort(struct ibv_context* context, uint8_t port_num) {
    struct ibv_port_attr port_attr;
    if (ibv_query_port(context, port_num, &port_attr) != 0) return false;
    if (port_attr.gid_tbl_len == 0) return false;
    if (port_attr.state != IBV_PORT_ACTIVE) return false;
    return true;
}

static bool isIbDeviceAvailable(struct ibv_device* device) {
    if (!isIbDeviceAccessible(device)) return false;
    struct ibv_context* context = ibv_open_device(device);
    if (!context) return false;
    struct ibv_device_attr device_attr;
    if (ibv_query_device(context, &device_attr) != 0) {
        ibv_close_device(context);
        return false;
    }
    bool has_active_port = false;
    for (uint8_t port = 1; port <= device_attr.phys_port_cnt; ++port) {
        if (checkIbDevicePort(context, port)) {
            has_active_port = true;
            break;
        }
    }
    ibv_close_device(context);
    return has_active_port;
}

static std::vector<Topology::NicEntry> listInfiniBandDevices() {
    int num_devices = 0;
    std::vector<Topology::NicEntry> devices;
    struct ibv_device** device_list = ibv_get_device_list(&num_devices);
    if (!device_list || num_devices <= 0) return {};

    for (int i = 0; i < num_devices; ++i) {
        std::string device_name = ibv_get_device_name(device_list[i]);
        if (!isIbDeviceAvailable(device_list[i])) continue;

        char path[PATH_MAX + 32];
        char resolved_path[PATH_MAX];
        snprintf(path, sizeof(path), "/sys/class/infiniband/%s/../..",
                 device_name.c_str());
        if (realpath(path, resolved_path) == NULL) continue;
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
    ibv_free_device_list(device_list);
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
    }
}

static void discoverCpuTopology(std::vector<Topology::NicEntry>& nic_list,
                                std::vector<Topology::MemEntry>& mem_list) {
    DIR* dir = opendir("/sys/devices/system/node");
    struct dirent* entry;
    if (dir == NULL) return;
    while ((entry = readdir(dir))) {
        const char* prefix = "node";
        if (entry->d_type != DT_DIR ||
            strncmp(entry->d_name, prefix, strlen(prefix)) != 0) {
            continue;
        }
        int numa_node = atoi(entry->d_name + strlen(prefix));
        Topology::MemEntry mem_entry;
        mem_entry.name = "cpu:" + std::to_string(numa_node);
        mem_entry.numa_node = numa_node;
        mem_entry.type = Topology::MEM_HOST;
        int nic_id = 0;
        for (const auto& device : nic_list) {
            if (device.numa_node == numa_node) {
                mem_entry.device_list[0].push_back(nic_id++);
            } else {
                mem_entry.device_list[2].push_back(nic_id++);
            }
        }
        mem_list.push_back(std::move(mem_entry));
    }
    (void)closedir(dir);
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

Status SunrisePlatform::probe(std::vector<Topology::NicEntry>& nic_list,
                              std::vector<Topology::MemEntry>& mem_list) {
    auto detected_nic = listInfiniBandDevices();
    filterInfiniBandDevices(detected_nic, conf);
    for (auto& entry : detected_nic) nic_list.push_back(entry);
    insertFallbackMemEntry((int)nic_list.size(), mem_list);
    discoverCpuTopology(nic_list, mem_list);

    int device_count = 0;
    if (tangGetDeviceCount(&device_count) != tangSuccess || device_count <= 0) {
        return Status::OK();
    }

    for (int i = 0; i < device_count; ++i) {
        Topology::MemEntry entry;
        entry.name = "cuda:" + std::to_string(i);
        entry.numa_node = -1;
        entry.type = Topology::MEM_CUDA;
        for (int nic = 0; nic < (int)nic_list.size(); ++nic) {
            entry.device_list[2].push_back(nic);
        }
        mem_list.push_back(std::move(entry));
    }
    return Status::OK();
}

MemoryType SunrisePlatform::getMemoryType(void* addr) {
    tangPointerAttributes attributes{};
    tangError_t ret = tangPointerGetAttributes(&attributes, addr);
    if (ret != tangSuccess) {
        int saved_dev = 0;
        if (tangGetDevice(&saved_dev) != tangSuccess) saved_dev = -1;
        int device_count = 0;
        if (tangGetDeviceCount(&device_count) == tangSuccess &&
            device_count > 0) {
            for (int d = 0; d < device_count; ++d) {
                if (tangSetDevice(d) != tangSuccess) continue;
                ret = tangPointerGetAttributes(&attributes, addr);
                if (ret == tangSuccess) break;
            }
            if (saved_dev >= 0) tangSetDevice(saved_dev);
        }
    }
    if (ret != tangSuccess) return MTYPE_CPU;
    if (attributes.type == tangMemoryTypeDevice) return MTYPE_CUDA;
    return MTYPE_CPU;
}

const std::vector<RangeLocation> SunrisePlatform::getLocation(
    void* start, size_t len, bool skip_prefault) {
    std::vector<RangeLocation> entries;

    tangPointerAttributes attributes{};
    tangError_t ret = tangPointerGetAttributes(&attributes, start);
    if (ret != tangSuccess) {
        int saved_dev = 0;
        if (tangGetDevice(&saved_dev) != tangSuccess) saved_dev = -1;
        int device_count = 0;
        if (tangGetDeviceCount(&device_count) == tangSuccess &&
            device_count > 0) {
            for (int d = 0; d < device_count; ++d) {
                if (tangSetDevice(d) != tangSuccess) continue;
                ret = tangPointerGetAttributes(&attributes, start);
                if (ret == tangSuccess) break;
            }
            if (saved_dev >= 0) tangSetDevice(saved_dev);
        }
    }

    if (ret == tangSuccess && attributes.type == tangMemoryTypeDevice) {
        entries.push_back(
            {(uint64_t)start, len, genCudaNodeName(attributes.device)});
        return entries;
    }

    const static size_t kPageSize = 4096;
    uintptr_t aligned_start = alignPage((uintptr_t)start);
    int n =
        (uintptr_t(start) - aligned_start + len + kPageSize - 1) / kPageSize;
    std::vector<void*> pages(n);
    std::vector<int> status(n);
    for (int i = 0; i < n; i++) {
        pages[i] = reinterpret_cast<void*>(aligned_start + i * kPageSize);
    }

    if (!skip_prefault) {
        prefaultBeforeProbe(pages.data(), n, aligned_start, "SunrisePlatform");
    }

    int rc = numa_move_pages(0, n, pages.data(), nullptr, status.data(), 0);
    if (rc != 0) {
        entries.push_back({(uint64_t)start, len, kWildcardLocation});
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
    return entries;
}

}  // namespace tent
}  // namespace mooncake

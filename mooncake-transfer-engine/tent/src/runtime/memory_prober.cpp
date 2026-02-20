// Copyright 2025 KVCache.AI
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

#include "tent/runtime/memory_prober.h"
#include "tent/device_plugin.h"
#include "tent/common/utils/prefault.h"

#include <filesystem>
#include <dlfcn.h>
#include <cstdlib>
#include <iostream>
#include <numa.h>
#include <limits.h>
#include <dirent.h>

namespace mooncake {
namespace tent {

namespace fs = std::filesystem;

Status MemoryProber::loadPlugins(const std::string& path) {
    unloadPlugins();
    std::error_code ec;
    if (!fs::exists(path, ec) || !fs::is_directory(path, ec)) {
        return Status::InvalidArgument("Plugin directory not found: " + path);
    }

    size_t count = 0;
    for (const auto& entry : fs::directory_iterator(path, ec)) {
        if (ec) break;
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() != ".so") continue;

        const std::string so_path = entry.path().string();
        void* handle = dlopen(so_path.c_str(), RTLD_NOW);
        if (!handle) {
            LOG(WARNING) << "[MemoryProber] dlopen failed for " << so_path
                         << ": " << dlerror();
            continue;
        }

        dlerror();
        auto reg_fn = reinterpret_cast<tent_register_device_plugin_fn>(
            dlsym(handle, TENT_DEVICE_PLUGIN_REGISTER_SYMBOL));
        const char* err = dlerror();
        if (err != nullptr || !reg_fn) {
            LOG(WARNING) << "[MemoryProber] dlsym(" << so_path
                         << ") failed: " << (err ? err : "null");
            dlclose(handle);
            continue;
        }

        device_plugin_t iface{};
        if (reg_fn(&iface) != 0) {
            LOG(WARNING) << "[MemoryProber] tent_register_device_plugin "
                            "failed for "
                         << so_path;
            dlclose(handle);
            continue;
        }

        void* ctx = iface.create_plugin();
        if (!ctx) {
            LOG(WARNING) << "[MemoryProber] create_plugin failed for "
                         << so_path << "\n";
            dlclose(handle);
            continue;
        }

        LoadedPlugin p;
        p.so_handle = handle;
        p.iface = iface;
        p.ctx = ctx;
        p.path = so_path;
        plugins_.push_back(std::move(p));
        ++count;
    }

    LOG(INFO) << "[MemoryProber] loaded " << count
              << " device plugins from: " << path;
    return Status::OK();
}

void MemoryProber::unloadPlugins() {
    for (auto& p : plugins_) {
        if (p.iface.destroy_plugin && p.ctx) {
            p.iface.destroy_plugin(p.ctx);
            p.ctx = nullptr;
        }
        if (p.so_handle) {
            dlclose(p.so_handle);
            p.so_handle = nullptr;
        }
    }
    plugins_.clear();
}

Status MemoryProber::listPlugins(std::vector<std::string>& plugins) {
    plugins.clear();
    for (auto& p : plugins_) {
        if (!p.iface.class_name) continue;
        plugins.push_back(p.iface.class_name);
    }
    return Status::OK();
}

Status MemoryProber::alloc(void** pptr, size_t size, const std::string& loc) {
    LocationParser location(loc);
    auto type = location.type();
    if (type.empty())
        return Status::InvalidArgument("Unrecognized location " + loc);

    if (type == kWildcardLocation || type == "cpu") {
        *pptr = numa_alloc_onnode(size, location.index());
        if (!(*pptr))
            return Status::InternalError("Unable to allocate DRAM memory");
        std::lock_guard<std::mutex> lock(mu_);
        memory_type_map_[*pptr] = "cpu";
        return Status::OK();
    }

    for (auto& p : plugins_) {
        if (!(p.iface.class_name == type) || !p.iface.alloc) continue;
        int rc = p.iface.alloc(p.ctx, pptr, size, loc.c_str());
        if (rc == 0) {
            std::lock_guard<std::mutex> lock(mu_);
            memory_type_map_[*pptr] = type;
            return Status::OK();
        }
    }

    return Status::InvalidArgument("No plugin for type " + type);
}

Status MemoryProber::free(void* ptr, size_t size) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!memory_type_map_.count(ptr))
        return Status::InvalidArgument("Not registered using MemoryProber");

    auto type = memory_type_map_[ptr];
    if (type == "cpu") {
        numa_free(ptr, size);
        memory_type_map_.erase(ptr);
        return Status::OK();
    }

    for (auto& p : plugins_) {
        if (!(p.iface.class_name == type) || !p.iface.free) continue;
        int rc = p.iface.free(p.ctx, ptr, size);
        if (rc == 0) {
            memory_type_map_.erase(ptr);
            return Status::OK();
        }
    }

    return Status::InvalidArgument("No plugin for type " + type);
}

Status MemoryProber::memcpy(void* dst, void* src, size_t size) {
    for (auto& p : plugins_) {
        if (!p.iface.memcpy_sync) continue;
        int rc = p.iface.memcpy_sync(p.ctx, dst, src, size);
        if (rc == 0) return Status::OK();
    }

    // If all plugins cannot execute memcpy_sync, use glibc
    ::memcpy(dst, src, size);
    return Status::OK();
}

static inline uintptr_t alignPage(uintptr_t address) {
    const static size_t kPageSize = 4096;
    return address & ~(kPageSize - 1);
}

static inline std::string genCpuNodeName(int node) {
    if (node >= 0) return "cpu:" + std::to_string(node);
    return kWildcardLocation;
}

const std::vector<RangeLocation> getCpuLocation(void* start, size_t len,
                                                bool skip_prefault = false) {
    const static size_t kPageSize = 4096;
    std::vector<RangeLocation> entries;

    // start and end address may not be page aligned.
    uintptr_t aligned_start = alignPage((uintptr_t)start);
    int n =
        (uintptr_t(start) - aligned_start + len + kPageSize - 1) / kPageSize;
    void** pages = (void**)malloc(sizeof(void*) * n);
    int* status = (int*)malloc(sizeof(int) * n);

    for (int i = 0; i < n; i++) {
        pages[i] = (void*)((char*)aligned_start + i * kPageSize);
    }

    // Prefault pages to reduce page-fault overhead during numa_move_pages.
    // Skip if caller has already pinned pages (e.g., via RDMA MR warm-up).
    if (!skip_prefault) {
        const PrefaultResult prefault_result =
            prefaultPages(pages, n, aligned_start, PrefaultOptions{});
        if (prefault_result.err != 0) {
            LOG(WARNING) << "[MemoryProber] Prefault " << prefault_result.method
                         << " failed with errno=" << prefault_result.err
                         << ", continuing with unprefaulted pages";
        } else {
            VLOG(1) << "[MemoryProber] Prefault succeeded: method="
                    << prefault_result.method
                    << " duration_ms=" << prefault_result.duration_ms
                    << " threads=" << prefault_result.threads
                    << " chunk_bytes=" << prefault_result.chunk_bytes;
        }
    }

    int rc = numa_move_pages(0, n, pages, nullptr, status, 0);
    if (rc != 0) {
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

const std::vector<RangeLocation> MemoryProber::locate(void* addr, size_t size,
                                                      bool skip_prefault) {
    location_t list_buf[64];
    for (auto& p : plugins_) {
        if (!p.iface.query_location) continue;
        int rc = p.iface.query_location(p.ctx, addr, size, list_buf, 64);
        if (rc > 0) {
            std::vector<RangeLocation> out;
            for (int i = 0; i < rc; ++i) {
                out.push_back(RangeLocation{(uint64_t)list_buf[i].start,
                                            list_buf[i].length,
                                            list_buf[i].location});
            }
            return out;
        }
    }
    return getCpuLocation(addr, size, skip_prefault);
}

std::string MemoryProber::type(void* addr) {
    for (auto& p : plugins_) {
        location_t list_buf;
        if (!p.iface.query_location) continue;
        int rc = p.iface.query_location(p.ctx, addr, 1, &list_buf, 1);
        if (rc > 0) return LocationParser(list_buf.location).type();
    }
    return "cpu";
}

static void probeHostMemory(const std::vector<Topology::NicEntry>& nic_list,
                            std::vector<Topology::MemEntry>& mem_list) {
    DIR* dir = opendir("/sys/devices/system/node");
    if (!dir) {
        PLOG(WARNING) << "open /sys/devices/system/node failed";
        return;
    }

    constexpr const char* kNodePrefix = "node";
    const int nic_count = static_cast<int>(nic_list.size());

    struct dirent* dent = nullptr;
    while ((dent = readdir(dir)) != nullptr) {
        if (strncmp(dent->d_name, kNodePrefix, strlen(kNodePrefix)) != 0) {
            continue;
        }

        int numa_node = atoi(dent->d_name + strlen(kNodePrefix));

        Topology::MemEntry mem;
        mem.name = "cpu:" + std::to_string(numa_node);
        mem.numa_node = numa_node;
        mem.type = Topology::MEM_HOST;

        for (int nic_id = 0; nic_id < nic_count; ++nic_id) {
            if (nic_list[nic_id].numa_node == numa_node) {
                mem.device_list[0].push_back(nic_id);
            } else {
                mem.device_list[2].push_back(nic_id);
            }
        }

        mem_list.push_back(std::move(mem));
    }

    (void)closedir(dir);

    // ensure wildcard entry exists and covers all NICs in device_list[2]
    for (auto& mem : mem_list) {
        if (mem.name == kWildcardLocation) {
            mem.device_list[2].clear();
            for (int i = 0; i < nic_count; ++i) mem.device_list[2].push_back(i);
            return;
        }
    }

    Topology::MemEntry wildcard;
    wildcard.name = kWildcardLocation;
    wildcard.numa_node = -1;
    wildcard.type = Topology::MEM_HOST;
    for (int i = 0; i < nic_count; ++i) wildcard.device_list[2].push_back(i);
    mem_list.push_back(std::move(wildcard));
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

void MemoryProber::probeDeviceMemory(
    MemoryProber::LoadedPlugin& plugin,
    const std::vector<Topology::NicEntry>& nic_list,
    std::vector<Topology::MemEntry>& mem_list) {
    if (!plugin.iface.get_device_count || !plugin.iface.get_device_pci_bus_id) {
        return;
    }
    int device_count = plugin.iface.get_device_count(plugin.ctx);
    if (device_count == 0) return;
    for (int i = 0; i < device_count; i++) {
        char pci_bus_id[20];
        int err = plugin.iface.get_device_pci_bus_id(plugin.ctx, i, pci_bus_id,
                                                     sizeof(pci_bus_id));
        if (err) continue;

        for (char* ch = pci_bus_id; (*ch = tolower(*ch)); ch++)
            ;
        int numa_node = getNumaNodeFromPciDevice(pci_bus_id);
        int min_distance = INT_MAX;
        std::unordered_map<int, std::vector<int>> distance_map;
        for (const auto& device : nic_list) {
            int dist = getPciDistance(device.pci_bus_id.c_str(), pci_bus_id);
            distance_map[dist].push_back(&device - &nic_list[0]);
            min_distance = std::min(min_distance, dist);
        }

        Topology::MemEntry entry;
        entry.name =
            std::string(plugin.iface.class_name) + ":" + std::to_string(i);
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

Status MemoryProber::probe(const std::vector<Topology::NicEntry>& nic_list,
                           std::vector<Topology::MemEntry>& mem_list) {
    probeHostMemory(nic_list, mem_list);
    for (auto& p : plugins_) probeDeviceMemory(p, nic_list, mem_list);
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake

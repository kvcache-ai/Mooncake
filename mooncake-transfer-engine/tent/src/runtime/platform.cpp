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

/**
 * @file platform.cpp
 * @brief Platform-independent core implementation
 *
 * This file contains NO platform-specific code. All GPU/NPU operations
 * are delegated to dynamically loaded backend plugins.
 */

#include "tent/runtime/platform.h"
#include "tent/common/status.h"
#include "tent/common/utils/prefault.h"
#include "tent/common/utils/os.h"

#include <numa.h>
#include <glog/logging.h>
#include <fstream>
#include <cstring>
#include <dirent.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dlfcn.h>

// RDMA support is optional (no GPU dependency)
#ifdef USE_RDMA
#include <infiniband/verbs.h>
#endif

namespace mooncake {
namespace tent {

// ============================================================================
// Location cache for hot path optimization
// ============================================================================

// Simple thread-local cache for getLocation results with TTL
// Key: page-aligned address, Value: location string + timestamp
struct LocationCacheEntry {
    uintptr_t page_addr;
    std::string location;
    uint64_t timestamp_ns;  // Cache entry timestamp
};

static constexpr size_t kLocationCacheSize = 64;
static constexpr uint64_t kCacheTtlNs = 60 * 1000000000ULL;  // 60 seconds TTL

thread_local LocationCacheEntry tl_location_cache[kLocationCacheSize] = {};
thread_local bool tl_location_cache_initialized = false;

static inline std::string* lookupLocationCache(uintptr_t page_addr) {
    if (!tl_location_cache_initialized) return nullptr;

    // Simple direct-mapped cache with modulo
    size_t idx =
        (page_addr >> 12) % kLocationCacheSize;  // Use page bits for hash
    if (tl_location_cache[idx].page_addr == page_addr) {
        // Check if cache entry is still valid (within TTL)
        uint64_t now = getCurrentTimeInNano();
        if (now - tl_location_cache[idx].timestamp_ns < kCacheTtlNs) {
            return &tl_location_cache[idx].location;
        }
        // Cache expired, will be refreshed
    }
    return nullptr;
}

static inline void updateLocationCache(uintptr_t page_addr,
                                       const std::string& location) {
    if (!tl_location_cache_initialized) {
        // Initialize cache on first use
        for (size_t i = 0; i < kLocationCacheSize; ++i) {
            tl_location_cache[i].page_addr = 0;
            tl_location_cache[i].timestamp_ns = 0;
        }
        tl_location_cache_initialized = true;
    }

    size_t idx = (page_addr >> 12) % kLocationCacheSize;
    tl_location_cache[idx].page_addr = page_addr;
    tl_location_cache[idx].location = location;
    tl_location_cache[idx].timestamp_ns = getCurrentTimeInNano();
}

// ============================================================================
// CPU fallback implementation (platform-independent)
// ============================================================================

static inline uintptr_t alignPage(uintptr_t address) {
    const static size_t kPageSize = 4096;
    return address & ~(kPageSize - 1);
}

static inline std::string genCpuNodeName(int node) {
    if (node >= 0) return "cpu:" + std::to_string(node);
    return kWildcardLocation;
}

#ifdef USE_RDMA
// RDMA device discovery (used by both core and plugins)
static std::vector<Topology::NicEntry> listInfiniBandDevices();
static void filterInfiniBandDevices(std::vector<Topology::NicEntry>& devices,
                                    std::shared_ptr<Config> conf);
#endif

static void discoverCpuTopology(std::vector<Topology::NicEntry>& nic_list,
                                std::vector<Topology::MemEntry>& mem_list) {
    DIR* dir = opendir("/sys/devices/system/node");
    struct dirent* entry;
    if (dir == NULL) {
        LOG(WARNING) << "open /sys/devices/system/node failed";
        return;
    }
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
    closedir(dir);
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

Status Platform::probeCpu(std::vector<Topology::NicEntry>& nic_list,
                          std::vector<Topology::MemEntry>& mem_list) {
#ifdef USE_RDMA
    auto detected_nic = listInfiniBandDevices();
    filterInfiniBandDevices(detected_nic, config_);
    for (auto& entry : detected_nic) nic_list.push_back(entry);
#else
    (void)config_;  // Suppress unused warning
#endif
    insertFallbackMemEntry((int)nic_list.size(), mem_list);
    discoverCpuTopology(nic_list, mem_list);
    return Status::OK();
}

// ============================================================================
// Platform core implementation
// ============================================================================

Platform& Platform::getInstance(std::shared_ptr<Config> config) {
    static Platform instance(config);
    return instance;
}

Platform::Platform(std::shared_ptr<Config> config) : config_(config) {
    loadBackend();
}

Platform::~Platform() = default;

// Forward declare the extern "C" factory function from backend.cpp
extern "C" std::shared_ptr<mooncake::tent::IPlatformBackend>
CreatePlatformBackend();

Status Platform::loadBackend() {
#ifdef TENT_BUILTIN_TRANSPORTS
    // BUILTIN mode: backend.cpp is statically linked with current platform
    backend_ = CreatePlatformBackend();
    if (backend_) {
        auto status = backend_->initialize(config_);
        if (status.ok()) {
            LOG(INFO) << "Using builtin platform backend: " << backend_->name();
        } else {
            LOG(WARNING) << "Builtin backend init failed: "
                         << status.ToString();
            backend_.reset();
        }
    }
    return Status::OK();
#else
    // PLUGIN mode: Try to load platform backend plugins
    std::vector<std::string> platforms = {
        "libtent_platform_cuda",   "libtent_platform_musa",
        "libtent_platform_hip",    "libtent_platform_maca",
        "libtent_platform_ascend", "libtent_platform_cpu",
    };

    const char* search_paths[] = {
        "/usr/local/lib/tent/platform", "/usr/lib/tent/platform",
        "/opt/tent/lib/platform", "./lib/tent/platform", nullptr};

    for (const auto& lib_name : platforms) {
        for (int i = 0; search_paths[i] != nullptr; ++i) {
            std::string full_path =
                std::string(search_paths[i]) + "/" + lib_name + ".so";
            void* handle = dlopen(full_path.c_str(), RTLD_NOW | RTLD_LOCAL);
            if (handle) {
                auto create_func =
                    reinterpret_cast<std::shared_ptr<IPlatformBackend> (*)()>(
                        dlsym(handle, "CreatePlatformBackend"));
                if (create_func) {
                    backend_ = create_func();
                    if (backend_ && backend_->initialize(config_).ok()) {
                        LOG(INFO)
                            << "Loaded platform backend: " << backend_->name()
                            << " from " << full_path;
                        return Status::OK();
                    }
                }
                dlclose(handle);
            }
        }
    }

    LOG(INFO) << "No platform backend plugin found, using CPU-only mode";
    return Status::OK();
#endif
}

Status Platform::probe(std::vector<Topology::NicEntry>& nic_list,
                       std::vector<Topology::MemEntry>& mem_list) {
#ifdef USE_RDMA
    auto detected_nic = listInfiniBandDevices();
    filterInfiniBandDevices(detected_nic, config_);
    for (auto& entry : detected_nic) nic_list.push_back(entry);
#endif
    insertFallbackMemEntry((int)nic_list.size(), mem_list);
    discoverCpuTopology(nic_list, mem_list);
    if (backend_) {
        auto ret = backend_->probe(nic_list, mem_list);
        if (!ret.ok()) return ret;
    }
    return Status::OK();
}

Status Platform::allocate(void** pptr, size_t size, MemoryOptions& options) {
    if (backend_) {
        return backend_->allocate(pptr, size, options);
    }

    // CPU fallback
    LocationParser location(options.location);
    int socket_id = 0;
    if (location.type() == "cpu") socket_id = location.index();
    *pptr = numa_alloc_onnode(size, socket_id);
    if (!(*pptr))
        return Status::InternalError("Unable to allocate DRAM memory");
    return Status::OK();
}

Status Platform::free(void* ptr, size_t size) {
    if (backend_) {
        return backend_->free(ptr, size);
    }
    numa_free(ptr, size);
    return Status::OK();
}

Status Platform::copy(void* dst, void* src, size_t length) {
    if (backend_) {
        return backend_->copy(dst, src, length);
    }
    memcpy(dst, src, length);
    return Status::OK();
}

MemoryType Platform::getMemoryType(void* addr) {
    if (backend_) {
        return backend_->getMemoryType(addr);
    }
    (void)addr;
    return MTYPE_CPU;
}

const std::vector<RangeLocation> Platform::getLocation(void* start, size_t len,
                                                       bool skip_prefault) {
    std::vector<RangeLocation> entries;

    // Unified cache for both backend and CPU fallback results
    const static size_t kPageSize = 4096;
    uintptr_t aligned_start = alignPage((uintptr_t)start);

    // Fast path: single page query - check cache first (works for any backend)
    if (len <= kPageSize) {
        std::string* cached = lookupLocationCache(aligned_start);
        if (cached) {
            entries.push_back({(uint64_t)start, len, *cached});
            return entries;
        }
    }

    // Cache miss: query backend or CPU fallback
    if (backend_) {
        entries = backend_->getLocation(start, len, skip_prefault);
        if (!entries.empty()) {
            // Cache the backend result for single-page queries
            if (len <= kPageSize && entries.size() == 1) {
                updateLocationCache(aligned_start, entries[0].location);
            }
            return entries;
        }
    }

    // CPU fallback with cache update
    int n =
        (uintptr_t(start) - aligned_start + len + kPageSize - 1) / kPageSize;
    void** pages = (void**)malloc(sizeof(void*) * n);
    int* status = (int*)malloc(sizeof(int) * n);

    for (int i = 0; i < n; i++) {
        pages[i] = (void*)((char*)aligned_start + i * kPageSize);
    }

    if (!skip_prefault) {
        prefaultBeforeProbe(pages, n, aligned_start, "Platform");
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
            std::string location = genCpuNodeName(node);
            entries.push_back(
                {start_addr, size_t(new_start_addr - start_addr), location});
            // Cache the location for this page range
            uintptr_t page_start = alignPage(start_addr);
            updateLocationCache(page_start, location);
            start_addr = new_start_addr;
            node = status[i];
        }
    }
    std::string final_location = genCpuNodeName(node);
    entries.push_back(
        {start_addr, (uint64_t)start + len - start_addr, final_location});
    // Cache the final location
    uintptr_t final_page_start = alignPage(start_addr);
    updateLocationCache(final_page_start, final_location);

    ::free(pages);
    ::free(status);
    return entries;
}

const std::string Platform::type() const {
    if (backend_) {
        auto prefix = backend_->getPrefix();
        // Remove trailing ':' if present
        if (!prefix.empty() && prefix.back() == ':')
            return prefix.substr(0, prefix.length() - 1);
        return prefix;
    }
    return "cpu";
}

// ============================================================================
// RDMA helper functions (optional, platform-independent)
// ============================================================================

#ifdef USE_RDMA

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

static bool checkIbDevicePort(struct ibv_context* context,
                              const std::string& device_name,
                              uint8_t port_num) {
    struct ibv_port_attr port_attr;
    if (ibv_query_port(context, port_num, &port_attr) != 0) return false;
    if (port_attr.gid_tbl_len == 0) return false;
    if (port_attr.state != IBV_PORT_ACTIVE) return false;
    return true;
}

static bool isIbDeviceAvailable(struct ibv_device* device) {
    const char* device_name = ibv_get_device_name(device);
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
        if (checkIbDevicePort(context, device_name, port)) {
            has_active_port = true;
            break;
        }
    }

    ibv_close_device(context);
    return has_active_port;
}

static double getLinkSpeedGbps(struct ibv_context* context, uint8_t port_num) {
    struct ibv_port_attr port_attr;
    if (ibv_query_port(context, port_num, &port_attr) != 0) return 200.0;

    // Convert IBV active_speed enum to total link bandwidth in Gbps
    switch (port_attr.active_speed) {
        case 1:
            return 10.0;  // SDR: 2.5 Gbps/lane * 4 = 10 Gbps
        case 2:
            return 20.0;  // DDR: 5 Gbps/lane * 4 = 20 Gbps
        case 4:
            return 40.0;  // QDR: 10 Gbps/lane * 4 = 40 Gbps
        case 8:
            return 40.0;  // FDR10: 10.3125 Gbps/lane * 4 = ~40 Gbps
        case 16:
            return 56.0;  // FDR: 14.0625 Gbps/lane * 4 = 56 Gbps
        case 32:
            return 100.0;  // EDR: 25.78125 Gbps/lane * 4 = 100 Gbps
        case 64:
            return 200.0;  // HDR: 25 Gbps/lane * 8 = 200 Gbps
        case 128:
            return 400.0;  // NDR: 50 Gbps/lane * 8 = 400 Gbps
        default:
            return 200.0;
    }
}

static std::vector<Topology::NicEntry> listInfiniBandDevices() {
    int num_devices = 0;
    std::vector<Topology::NicEntry> devices;

    struct ibv_device** device_list = ibv_get_device_list(&num_devices);
    if (!device_list || num_devices <= 0) return {};

    for (int i = 0; i < num_devices; ++i) {
        if (!isIbDeviceAvailable(device_list[i])) continue;

        std::string device_name = ibv_get_device_name(device_list[i]);
        char path[PATH_MAX + 32];
        char resolved_path[PATH_MAX];
        snprintf(path, sizeof(path), "/sys/class/infiniband/%s/../..",
                 device_name.c_str());
        if (realpath(path, resolved_path) == NULL) continue;

        std::string pci_bus_id = basename(resolved_path);
        int numa_node = -1;
        snprintf(path, sizeof(path), "%s/numa_node", resolved_path);
        std::ifstream(path) >> numa_node;

        // Query link speed
        double bw_gbps = 200.0;  // Default HDR
        struct ibv_context* context = ibv_open_device(device_list[i]);
        if (context) {
            struct ibv_device_attr device_attr;
            if (ibv_query_device(context, &device_attr) == 0) {
                for (uint8_t port = 1; port <= device_attr.phys_port_cnt;
                     ++port) {
                    if (checkIbDevicePort(context, device_name.c_str(), port)) {
                        bw_gbps = getLinkSpeedGbps(context, port);
                        break;
                    }
                }
            }
            ibv_close_device(context);
        }

        devices.push_back(
            Topology::NicEntry{.name = std::move(device_name),
                               .pci_bus_id = std::move(pci_bus_id),
                               .type = Topology::NIC_RDMA,
                               .numa_node = numa_node,
                               .bw_gbps = bw_gbps});
    }
    ibv_free_device_list(device_list);
    return devices;
}

static void filterInfiniBandDevices(std::vector<Topology::NicEntry>& devices,
                                    std::shared_ptr<Config> conf) {
    if (!conf) return;
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
    } else if (!blacklist.empty()) {
        for (auto& entry : devices) {
            if (std::find(blacklist.begin(), blacklist.end(), entry.name) ==
                blacklist.end())
                new_devices.push_back(entry);
        }
        devices.swap(new_devices);
    }
}

#endif  // USE_RDMA

}  // namespace tent
}  // namespace mooncake

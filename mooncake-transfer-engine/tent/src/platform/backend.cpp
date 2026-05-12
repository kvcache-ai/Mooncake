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
 * @file backend.cpp
 * @brief Platform backend implementation
 *
 * This single file is compiled multiple times with different platform flags:
 *   -DUSE_CUDA   → libtent_platform_cuda.so
 *   -DUSE_MUSA   → libtent_platform_musa.so
 *   -DUSE_HIP    → libtent_platform_hip.so
 *   -DUSE_ASCEND → libtent_platform_ascend.so
 *
 * The gpu_vendor.h macros handle the API mapping to vendor-specific calls.
 */

#include "tent/runtime/platform.h"
#include "tent/platform/gpu_vendor.h"
#include "tent/common/status.h"
#include "tent/common/utils/prefault.h"

#include <numa.h>
#include <glog/logging.h>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <limits.h>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <cctype>

namespace mooncake {
namespace tent {

class PlatformBackend : public IPlatformBackend {
   public:
    std::string name() const override {
#if defined(USE_CUDA)
        return "CUDA";
#elif defined(USE_MUSA)
        return "MUSA";
#elif defined(USE_HIP)
        return "HIP";
#elif defined(USE_MACA)
        return "MACA";
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
        return "Ascend";
#else
        return "CPU";
#endif
    }

    Status initialize(std::shared_ptr<Config> config) override {
        config_ = config;

#if defined(HAS_DEVICE_SUPPORT)
        int count = 0;
        auto ret = cudaGetDeviceCount(&count);
        if (ret != CUDA_SUCCESS || count == 0) {
            LOG(WARNING) << name() << " not available";
            return Status::InternalError(name() + " not available");
        }
        LOG(INFO) << name() << " backend initialized with " << count
                  << " device(s)";
#else
        LOG(INFO) << "CPU backend initialized";
#endif
        return Status::OK();
    }

    Status probe(std::vector<Topology::NicEntry>& nic_list,
                 std::vector<Topology::MemEntry>& mem_list) override {
#if defined(HAS_DEVICE_SUPPORT)
        int device_count = 0;
        auto ret = cudaGetDeviceCount(&device_count);
        if (ret != CUDA_SUCCESS || device_count == 0) {
            return Status::OK();
        }
        LOG(INFO) << "Found " << device_count << " " << name() << " device(s)";
        for (int i = 0; i < device_count; ++i) {
            Topology::MemEntry entry;
            entry.name = getGpuPrefix() + std::to_string(i);

            // Set memory type based on platform
            entry.type = getMemType();

            // Get GPU PCI bus ID and NUMA node
            std::string gpu_pci_id = getGpuPciBusId(i);
            if (!gpu_pci_id.empty()) {
                entry.pci_bus_id = gpu_pci_id;
                entry.numa_node = getNumaNodeFromPciDevice(gpu_pci_id);
            }

            // Build tier-based GPU→NIC mappings using PCIe distance
            buildGpuNicMappings(entry, gpu_pci_id, entry.numa_node, nic_list);

            mem_list.push_back(entry);
        }
#endif
        return Status::OK();
    }

    Status allocate(void** pptr, size_t size, MemoryOptions& options) override {
        LocationParser location(options.location);
#if defined(HAS_DEVICE_SUPPORT)
        auto gpu_prefix = getGpuPrefix();
        // Check if location matches GPU type
        if (!location.type().empty() &&
            gpu_prefix.find(location.type() + ":") == 0) {
            int device = 0;
            auto err = cudaGetDevice(&device);
            if (err != CUDA_SUCCESS)
                return Status::InternalError("cudaGetDevice failed");
            err = cudaSetDevice(location.index());
            if (err != CUDA_SUCCESS)
                return Status::InternalError("cudaSetDevice failed");
            err = cudaMalloc(pptr, size);
            if (err != CUDA_SUCCESS)
                return Status::InternalError("cudaMalloc failed");
            cudaSetDevice(device);
            return Status::OK();
        }
#endif
        // CPU allocation (common to all platforms)
        int socket_id = 0;
        if (location.type() == "cpu") socket_id = location.index();
        *pptr = numa_alloc_onnode(size, socket_id);
        return *pptr ? Status::OK()
                     : Status::InternalError("numa_alloc failed");
    }

    Status free(void* ptr, size_t size) override {
#if defined(HAS_DEVICE_SUPPORT)
        cudaPointerAttributes attr;
        auto ret = cudaPointerGetAttributes(&attr, ptr);
        if (ret == CUDA_SUCCESS && attr.type == cudaMemoryTypeDevice) {
            auto err = cudaFree(ptr);
            return (err == CUDA_SUCCESS)
                       ? Status::OK()
                       : Status::InternalError("cudaFree failed");
        }
#endif
        numa_free(ptr, size);
        return Status::OK();
    }

    Status copy(void* dst, void* src, size_t length) override {
#if defined(HAS_DEVICE_SUPPORT)
        auto err = cudaMemcpy(dst, src, length, cudaMemcpyDefault);
        return (err == CUDA_SUCCESS)
                   ? Status::OK()
                   : Status::InternalError("cudaMemcpy failed");
#else
        memcpy(dst, src, length);
        return Status::OK();
#endif
    }

    MemoryType getMemoryType(void* addr) override {
#if defined(HAS_DEVICE_SUPPORT)
        cudaPointerAttributes attr;
        auto ret = cudaPointerGetAttributes(&attr, addr);
        if (ret == CUDA_SUCCESS && attr.type == cudaMemoryTypeDevice) {
#if defined(USE_CUDA)
            return MTYPE_CUDA;
#elif defined(USE_ASCEND)
            return MTYPE_ASCEND;
#elif defined(USE_HIP)
            return MTYPE_HIP;
#elif defined(USE_MUSA)
            return MTYPE_MUSA;
#elif defined(USE_MACA)
            return MTYPE_MACA;
#else
            return MTYPE_CUDA;  // Fallback
#endif
        }
#endif
        (void)addr;
        return MTYPE_CPU;
    }

    const std::vector<RangeLocation> getLocation(void* start, size_t len,
                                                 bool skip_prefault) override {
        std::vector<RangeLocation> locations;
#if defined(HAS_DEVICE_SUPPORT)
        cudaPointerAttributes attr;
        auto ret = cudaPointerGetAttributes(&attr, start);

        if (ret == CUDA_SUCCESS && attr.type == cudaMemoryTypeDevice) {
            RangeLocation rl;
            rl.start = reinterpret_cast<uint64_t>(start);
            rl.len = len;
            rl.location = getGpuPrefix() + std::to_string(attr.device);
            locations.push_back(rl);
            return locations;
        }
#endif
        return locations;
    }

    int getDeviceCount() const override {
#if defined(HAS_DEVICE_SUPPORT)
        int count = 0;
        cudaGetDeviceCount(&count);
        return count;
#else
        return 0;
#endif
    }

    std::string getPrefix() const override {
#if defined(HAS_DEVICE_SUPPORT)
        return getGpuPrefix();
#else
        return "cpu:";
#endif
    }

   private:
    std::shared_ptr<Config> config_;

    // Get memory type for this platform
    static Topology::MemType getMemType() {
#if defined(USE_CUDA)
        return Topology::MEM_CUDA;
#elif defined(USE_MUSA)
        return Topology::MEM_ROCM;  // MUSA uses ROCm-compatible memory type
#elif defined(USE_HIP)
        return Topology::MEM_ROCM;
#elif defined(USE_MACA)
        return Topology::MEM_ROCM;  // MACA uses ROCm-compatible memory type
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
        return Topology::MEM_ASCEND;
#else
        return Topology::MEM_HOST;
#endif
    }

    // Get GPU PCI bus ID (e.g., "0000:03:00.0")
    static std::string getGpuPciBusId(int device) {
#if defined(HAS_DEVICE_SUPPORT)
        char pci_bus_id[20];
        auto err =
            cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), device);
        if (err == CUDA_SUCCESS) {
            // Convert to lowercase for consistency
            for (char* ch = pci_bus_id; *ch; ++ch) *ch = tolower(*ch);
            return std::string(pci_bus_id);
        }
#endif
        return "";
    }

    // Get NUMA node from PCI device sysfs
    static int getNumaNodeFromPciDevice(const std::string& pci_bdf) {
        std::string sysfs_path =
            "/sys/bus/pci/devices/" + pci_bdf + "/numa_node";
        std::ifstream numa_file(sysfs_path);
        if (!numa_file.is_open()) return -1;
        int numa_node = -1;
        numa_file >> numa_node;
        if (numa_file.fail()) return -1;
        return numa_node;
    }

    // Calculate PCIe distance between two devices
    // Returns 0 if same PCIe switch/root complex, higher values for further
    // away
    static int getPciDistance(const std::string& bus1,
                              const std::string& bus2) {
        char buf[PATH_MAX];
        char path1[PATH_MAX];
        char path2[PATH_MAX];

        snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus1.c_str());
        if (realpath(buf, path1) == NULL) return -1;
        snprintf(buf, sizeof(buf), "/sys/bus/pci/devices/%s", bus2.c_str());
        if (realpath(buf, path2) == NULL) return -1;

        // Count path segments after the common prefix
        char* ptr1 = path1;
        char* ptr2 = path2;
        while (*ptr1 && *ptr1 == *ptr2) {
            ptr1++;
            ptr2++;
        }

        int distance = 0;
        for (; *ptr1; ptr1++) distance += (*ptr1 == '/');
        for (; *ptr2; ptr2++) distance += (*ptr2 == '/');

        return distance;
    }

    // Build tier-based GPU→NIC mappings
    // device_list[0]: Same PCIe switch/RC (distance 0 or closest)
    // device_list[1]: Same NUMA but not in device_list[0]
    // device_list[2]: Cross-NUMA devices
    void buildGpuNicMappings(Topology::MemEntry& entry,
                             const std::string& gpu_pci_id, int gpu_numa_node,
                             const std::vector<Topology::NicEntry>& nic_list) {
        if (gpu_pci_id.empty() || nic_list.empty()) return;

        int min_distance = INT_MAX;
        std::unordered_map<int, std::vector<int>> distance_map;

        // Calculate PCIe distance to each NIC
        for (size_t i = 0; i < nic_list.size(); ++i) {
            int dist = getPciDistance(nic_list[i].pci_bus_id, gpu_pci_id);
            if (dist >= 0) {
                distance_map[dist].push_back(i);
                min_distance = std::min(min_distance, dist);
            }
        }

        // Tier 0: NICs with distance 0 (same PCIe switch/RC) or closest
        if (distance_map.count(0)) {
            entry.device_list[0] = std::move(distance_map[0]);
        } else if (distance_map.count(min_distance)) {
            entry.device_list[0] = std::move(distance_map[min_distance]);
        }

        // Build set of Tier 0 devices for exclusion
        std::unordered_set<int> tier0_set;
        for (int dev_id : entry.device_list[0]) {
            tier0_set.insert(dev_id);
        }

        // Tier 1: Same NUMA but not in Tier 0
        // Tier 2: Cross-NUMA devices
        for (size_t i = 0; i < nic_list.size(); ++i) {
            if (tier0_set.count(i)) continue;  // Skip Tier 0 devices

            if (gpu_numa_node >= 0 && nic_list[i].numa_node == gpu_numa_node) {
                entry.device_list[1].push_back(i);
            } else {
                entry.device_list[2].push_back(i);
            }
        }
    }
};

// Export the backend - symbol name is the same for all platforms
extern "C" {
std::shared_ptr<mooncake::tent::IPlatformBackend> CreatePlatformBackend() {
    return std::make_shared<PlatformBackend>();
}
}

}  // namespace tent
}  // namespace mooncake

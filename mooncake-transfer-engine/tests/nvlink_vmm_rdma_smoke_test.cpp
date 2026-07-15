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

#include <gtest/gtest.h>
#include <infiniband/verbs.h>

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <set>
#include <string>

#include <sys/stat.h>

#include "cuda_alike.h"
#include "transport/nvlink_transport/nvlink_transport.h"

namespace mooncake {
namespace {

constexpr size_t kRegistrationBytes = 2U << 20;

bool ParseStrictMode(const char* name, bool& strict, std::string& error) {
    strict = false;
    const char* value = std::getenv(name);
    if (value == nullptr || std::string(value) == "0") return true;
    if (std::string(value) == "1") {
        strict = true;
        return true;
    }
    error = std::string(name) + " must be 0 or 1";
    return false;
}

bool ParseNonNegativeInt(const std::string& text, int& value) {
    if (text.empty()) return false;
    size_t parsed = 0;
    try {
        const long result = std::stol(text, &parsed, 10);
        if (parsed != text.size() || result < 0 ||
            result > std::numeric_limits<int>::max()) {
            return false;
        }
        value = static_cast<int>(result);
        return true;
    } catch (...) {
        return false;
    }
}

bool IsOnlineNumaNode(int node, std::string& error) {
    const std::string node_path =
        "/sys/devices/system/node/node" + std::to_string(node);
    struct stat node_stat{};
    if (stat(node_path.c_str(), &node_stat) != 0 ||
        !S_ISDIR(node_stat.st_mode)) {
        error = "NUMA node " + std::to_string(node) + " is absent from sysfs";
        return false;
    }

    std::ifstream online(node_path + "/online");
    if (!online.is_open()) return true;
    int flag = 0;
    if (!(online >> flag) || flag != 1) {
        error = "NUMA node " + std::to_string(node) + " is not online";
        return false;
    }
    return true;
}

bool DeviceNumaNodeFromSysfs(int device, int& node, std::string& error) {
    char pci_bus_id[64] = {};
    cudaError_t result =
        cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), device);
    if (result != cudaSuccess) {
        error = "cudaDeviceGetPCIBusId failed for visible GPU " +
                std::to_string(device) + ": " + cudaGetErrorString(result);
        return false;
    }

    std::string bdf(pci_bus_id);
    std::transform(bdf.begin(), bdf.end(), bdf.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (std::count(bdf.begin(), bdf.end(), ':') == 1) bdf = "0000:" + bdf;

    std::ifstream numa_file("/sys/bus/pci/devices/" + bdf + "/numa_node");
    if (!numa_file.is_open() || !(numa_file >> node) || node < 0) {
        error = "visible GPU " + std::to_string(device) +
                " has no usable PCI-to-NUMA mapping in sysfs";
        return false;
    }
    return IsOnlineNumaNode(node, error);
}

bool SelectHostNumaNode(int& node, std::string& source, std::string& error) {
    int device_count = 0;
    const cudaError_t result = cudaGetDeviceCount(&device_count);
    if (result != cudaSuccess) {
        error = std::string("cudaGetDeviceCount failed: ") +
                cudaGetErrorString(result);
        return false;
    }
    if (device_count <= 0) {
        error = "no visible CUDA GPU";
        return false;
    }

    const char* override_node = std::getenv("MC_NVLINK_VMM_TEST_NODE");
    if (override_node != nullptr && *override_node != '\0') {
        if (!ParseNonNegativeInt(override_node, node)) {
            error =
                "MC_NVLINK_VMM_TEST_NODE must be a non-negative "
                "integer";
            return false;
        }
        if (!IsOnlineNumaNode(node, error)) return false;
        source = "MC_NVLINK_VMM_TEST_NODE";
        return true;
    }

    std::set<int> gpu_nodes;
    for (int device = 0; device < device_count; ++device) {
        int gpu_node = -1;
        if (!DeviceNumaNodeFromSysfs(device, gpu_node, error)) return false;
        gpu_nodes.insert(gpu_node);
    }
    if (gpu_nodes.empty()) {
        error = "visible GPU PCI sysfs discovery produced no online NUMA node";
        return false;
    }
    node = *gpu_nodes.begin();
    source = "visible GPU PCI sysfs";
    return true;
}

class DeviceList {
   public:
    DeviceList() { devices_ = ibv_get_device_list(&count_); }
    DeviceList(const DeviceList&) = delete;
    DeviceList& operator=(const DeviceList&) = delete;
    ~DeviceList() {
        if (devices_ != nullptr) ibv_free_device_list(devices_);
    }

    ibv_device** get() const { return devices_; }
    int count() const { return count_; }

   private:
    ibv_device** devices_ = nullptr;
    int count_ = 0;
};

class VerbsContext {
   public:
    VerbsContext() = default;
    VerbsContext(const VerbsContext&) = delete;
    VerbsContext& operator=(const VerbsContext&) = delete;
    ~VerbsContext() { (void)Reset(); }

    bool Open(ibv_device* device) {
        context_ = ibv_open_device(device);
        return context_ != nullptr;
    }

    int Reset() {
        if (context_ == nullptr) return 0;
        const int result = ibv_close_device(context_);
        if (result == 0) context_ = nullptr;
        return result;
    }

    ibv_context* get() const { return context_; }

   private:
    ibv_context* context_ = nullptr;
};

class ProtectionDomain {
   public:
    ProtectionDomain() = default;
    ProtectionDomain(const ProtectionDomain&) = delete;
    ProtectionDomain& operator=(const ProtectionDomain&) = delete;
    ~ProtectionDomain() { (void)Reset(); }

    bool Allocate(ibv_context* context) {
        domain_ = ibv_alloc_pd(context);
        return domain_ != nullptr;
    }

    int Reset() {
        if (domain_ == nullptr) return 0;
        const int result = ibv_dealloc_pd(domain_);
        if (result == 0) domain_ = nullptr;
        return result;
    }

    ibv_pd* get() const { return domain_; }

   private:
    ibv_pd* domain_ = nullptr;
};

class MemoryRegion {
   public:
    MemoryRegion() = default;
    MemoryRegion(const MemoryRegion&) = delete;
    MemoryRegion& operator=(const MemoryRegion&) = delete;
    ~MemoryRegion() { (void)Reset(); }

    bool Register(ibv_pd* domain, void* address, size_t length) {
        region_ = ibv_reg_mr(domain, address, length,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                 IBV_ACCESS_REMOTE_WRITE);
        return region_ != nullptr;
    }

    int Reset() {
        if (region_ == nullptr) return 0;
        const int result = ibv_dereg_mr(region_);
        if (result == 0) region_ = nullptr;
        return result;
    }

   private:
    ibv_mr* region_ = nullptr;
};

ibv_device* SelectRdmaDevice(const DeviceList& list, std::string& name,
                             std::string& error) {
    if (list.get() == nullptr) {
        error =
            "ibv_get_device_list failed: " + std::string(std::strerror(errno));
        return nullptr;
    }
    if (list.count() <= 0) {
        error = "no RNIC is visible through libibverbs";
        return nullptr;
    }

    const char* requested = std::getenv("MC_NVLINK_VMM_RDMA_DEVICE");
    if (requested == nullptr || *requested == '\0') {
        const char* first_name = ibv_get_device_name(list.get()[0]);
        if (first_name == nullptr) {
            error = "the first libibverbs device has no name";
            return nullptr;
        }
        name = first_name;
        return list.get()[0];
    }

    for (int index = 0; index < list.count(); ++index) {
        const char* candidate = ibv_get_device_name(list.get()[index]);
        if (candidate != nullptr && std::string(candidate) == requested) {
            name = candidate;
            return list.get()[index];
        }
    }
    error = "requested RNIC is not visible: " + std::string(requested);
    return nullptr;
}

int ReadRdmaDeviceNumaNode(const std::string& device_name) {
    std::ifstream numa_file("/sys/class/infiniband/" + device_name +
                            "/device/numa_node");
    int node = -1;
    if (!(numa_file >> node)) return -1;
    return node;
}

#define REQUIRE_RDMA_OR_SKIP(strict, reason) \
    do {                                     \
        if (strict) {                        \
            FAIL() << (reason);              \
        } else {                             \
            GTEST_SKIP() << (reason);        \
        }                                    \
    } while (false)

TEST(NvlinkVmmRdmaSmokeTest, VerbsCanRegisterProviderVmmAllocation) {
    bool strict = false;
    std::string prerequisite_error;
    ASSERT_TRUE(ParseStrictMode("MC_REQUIRE_NVLINK_VMM_RDMA", strict,
                                prerequisite_error))
        << prerequisite_error;

    int host_numa_node = -1;
    std::string node_source;
    if (!SelectHostNumaNode(host_numa_node, node_source, prerequisite_error)) {
        REQUIRE_RDMA_OR_SKIP(strict, prerequisite_error);
    }

    Status capability = NvlinkVmmAllocation::CheckStrictFabricCapability();
    if (!capability.ok()) {
        REQUIRE_RDMA_OR_SKIP(
            strict, "Fabric-capable HOST_NUMA VMM prerequisite failed: " +
                        capability.ToString());
    }

    NvlinkVmmAllocation::Options options;
    options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
    options.location_id = host_numa_node;
    options.requested_length = kRegistrationBytes;
    options.fabric_exportable = true;

    std::unique_ptr<NvlinkVmmAllocation> allocation;
    Status allocation_status = NvlinkVmmAllocation::Create(options, allocation);
    if (!allocation_status.ok()) {
        REQUIRE_RDMA_OR_SKIP(
            strict, "HOST_NUMA Fabric VMM allocation prerequisite failed via " +
                        node_source + ": " + allocation_status.ToString());
    }
    ASSERT_NE(allocation, nullptr);
    ASSERT_NE(allocation->base(), nullptr);
    ASSERT_GE(allocation->length(), kRegistrationBytes);
    std::memset(allocation->base(), 0x5a, kRegistrationBytes);

    DeviceList devices;
    std::string rdma_device_name;
    ibv_device* device =
        SelectRdmaDevice(devices, rdma_device_name, prerequisite_error);
    if (device == nullptr) {
        REQUIRE_RDMA_OR_SKIP(strict, prerequisite_error);
    }

    ::testing::Test::RecordProperty("host_numa_node",
                                    std::to_string(host_numa_node));
    ::testing::Test::RecordProperty("rdma_device", rdma_device_name);
    ::testing::Test::RecordProperty(
        "rdma_device_numa_node",
        std::to_string(ReadRdmaDeviceNumaNode(rdma_device_name)));

    VerbsContext context;
    if (!context.Open(device)) {
        const std::string error = "ibv_open_device failed for " +
                                  rdma_device_name + ": " +
                                  std::strerror(errno);
        REQUIRE_RDMA_OR_SKIP(strict, error);
    }

    ProtectionDomain domain;
    if (!domain.Allocate(context.get())) {
        const std::string error = "ibv_alloc_pd failed for " +
                                  rdma_device_name + ": " +
                                  std::strerror(errno);
        REQUIRE_RDMA_OR_SKIP(strict, error);
    }

    MemoryRegion region;
    if (!region.Register(domain.get(), allocation->base(),
                         allocation->length())) {
        const std::string error =
            "ibv_reg_mr rejected the HOST_NUMA VMM allocation on " +
            rdma_device_name + ": " + std::strerror(errno);
        REQUIRE_RDMA_OR_SKIP(strict, error);
    }

    ASSERT_EQ(region.Reset(), 0) << "ibv_dereg_mr failed";
    ASSERT_EQ(domain.Reset(), 0) << "ibv_dealloc_pd failed";
    ASSERT_EQ(context.Reset(), 0) << "ibv_close_device failed";
    allocation.reset();
    ::testing::Test::RecordProperty("result", "ibv_reg_mr_and_dereg_mr_pass");
}

#undef REQUIRE_RDMA_OR_SKIP

}  // namespace
}  // namespace mooncake

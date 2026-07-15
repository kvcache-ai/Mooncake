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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <limits>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <sys/stat.h>

#include "cuda_alike.h"
#include "error.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/nvlink_transport/nvlink_transport.h"
#include "transport/transport.h"

namespace mooncake {

#if defined(USE_MNNVL) && defined(USE_CUDA)
struct FabricImportAudit {
    std::atomic<bool> fail_next_map{true};
    std::atomic<int> import_calls{0};
    std::atomic<int> injected_map_failures{0};
    std::atomic<int> live_handles{0};
    std::atomic<int> live_reserved_ranges{0};
    std::atomic<int> live_mappings{0};
};

class NvlinkTransportTestPeer {
   public:
    static void InstallOneShotMapFailure(
        NvlinkTransport& transport,
        const std::shared_ptr<FabricImportAudit>& audit) {
        auto api = transport.fabric_driver_api_;
        auto original_import = api.mem_import_from_shareable_handle;
        auto original_reserve = api.mem_address_reserve;
        auto original_map = api.mem_map;
        auto original_unmap = api.mem_unmap;
        auto original_free = api.mem_address_free;
        auto original_release = api.mem_release;

        api.mem_import_from_shareable_handle =
            [audit, original_import](CUmemGenericAllocationHandle* handle,
                                     void* shareable,
                                     CUmemAllocationHandleType type) {
                ++audit->import_calls;
                const CUresult result =
                    original_import(handle, shareable, type);
                if (result == CUDA_SUCCESS) ++audit->live_handles;
                return result;
            };
        api.mem_address_reserve = [audit, original_reserve](
                                      CUdeviceptr* address, size_t size,
                                      size_t alignment, CUdeviceptr requested,
                                      unsigned long long flags) {
            const CUresult result =
                original_reserve(address, size, alignment, requested, flags);
            if (result == CUDA_SUCCESS) ++audit->live_reserved_ranges;
            return result;
        };
        api.mem_map = [audit, original_map](CUdeviceptr address, size_t size,
                                            size_t offset,
                                            CUmemGenericAllocationHandle handle,
                                            unsigned long long flags) {
            if (audit->fail_next_map.exchange(false)) {
                ++audit->injected_map_failures;
                return CUDA_ERROR_INVALID_VALUE;
            }
            const CUresult result =
                original_map(address, size, offset, handle, flags);
            if (result == CUDA_SUCCESS) ++audit->live_mappings;
            return result;
        };
        api.mem_unmap = [audit, original_unmap](CUdeviceptr address,
                                                size_t size) {
            const CUresult result = original_unmap(address, size);
            if (result == CUDA_SUCCESS) --audit->live_mappings;
            return result;
        };
        api.mem_address_free = [audit, original_free](CUdeviceptr address,
                                                      size_t size) {
            const CUresult result = original_free(address, size);
            if (result == CUDA_SUCCESS) --audit->live_reserved_ranges;
            return result;
        };
        api.mem_release =
            [audit, original_release](CUmemGenericAllocationHandle handle) {
                const CUresult result = original_release(handle);
                if (result == CUDA_SUCCESS) --audit->live_handles;
                return result;
            };
        transport.fabric_driver_api_ = std::move(api);
    }

    static size_t MappingCount(const NvlinkTransport& transport) {
        return transport.remap_entries_.size();
    }
};
#endif

namespace {

constexpr size_t kTransferBytes = 1U << 20;
constexpr auto kTransferTimeout = std::chrono::seconds(30);

struct HardwareSelection {
    std::vector<int> devices;
    std::vector<int> numa_nodes;
    std::string source;
};

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

bool ParseNumaNodeList(const std::string& text, std::vector<int>& nodes,
                       std::string& error) {
    if (text.empty()) {
        error = "NUMA node list must not be empty";
        return false;
    }

    std::set<int> unique_nodes;
    std::stringstream input(text);
    std::string token;
    while (std::getline(input, token, ',')) {
        int node = -1;
        if (!ParseNonNegativeInt(token, node)) {
            error =
                "NUMA node list must contain comma-separated "
                "non-negative integers";
            return false;
        }
        unique_nodes.insert(node);
    }
    if (unique_nodes.empty()) {
        error = "NUMA node list must contain at least one node";
        return false;
    }
    nodes.assign(unique_nodes.begin(), unique_nodes.end());
    return true;
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
    if (!online.is_open()) {
        // Linux commonly omits node0/online. An existing node directory is
        // online in that case.
        return true;
    }
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

    const std::string numa_path = "/sys/bus/pci/devices/" + bdf + "/numa_node";
    std::ifstream numa_file(numa_path);
    if (!numa_file.is_open() || !(numa_file >> node) || node < 0) {
        error = "visible GPU " + std::to_string(device) +
                " has no usable PCI-to-NUMA mapping in sysfs";
        return false;
    }
    return IsOnlineNumaNode(node, error);
}

bool SelectHardware(HardwareSelection& selection, std::string& error) {
    int device_count = 0;
    cudaError_t runtime_result = cudaGetDeviceCount(&device_count);
    if (runtime_result != cudaSuccess) {
        error = std::string("cudaGetDeviceCount failed: ") +
                cudaGetErrorString(runtime_result);
        return false;
    }
    if (device_count <= 0) {
        error = "no visible CUDA GPU";
        return false;
    }
    selection.devices.reserve(device_count);
    for (int device = 0; device < device_count; ++device)
        selection.devices.push_back(device);

    const char* override_nodes = std::getenv("MC_NVLINK_VMM_TEST_NODES");
    const char* override_node = std::getenv("MC_NVLINK_VMM_TEST_NODE");
    const bool has_node_list =
        override_nodes != nullptr && *override_nodes != '\0';
    const bool has_single_node =
        override_node != nullptr && *override_node != '\0';
    if (has_node_list && has_single_node) {
        error =
            "set only one of MC_NVLINK_VMM_TEST_NODES and "
            "MC_NVLINK_VMM_TEST_NODE";
        return false;
    }
    if (has_node_list) {
        if (!ParseNumaNodeList(override_nodes, selection.numa_nodes, error))
            return false;
        for (int node : selection.numa_nodes) {
            if (!IsOnlineNumaNode(node, error)) return false;
        }
        selection.source = "MC_NVLINK_VMM_TEST_NODES";
        return true;
    }
    if (has_single_node) {
        int node = -1;
        if (!ParseNonNegativeInt(override_node, node)) {
            error =
                "MC_NVLINK_VMM_TEST_NODE must be a non-negative "
                "integer";
            return false;
        }
        if (!IsOnlineNumaNode(node, error)) return false;
        selection.numa_nodes = {node};
        selection.source = "MC_NVLINK_VMM_TEST_NODE";
        return true;
    }

    std::set<int> gpu_nodes;
    for (int device : selection.devices) {
        int node = -1;
        if (!DeviceNumaNodeFromSysfs(device, node, error)) return false;
        gpu_nodes.insert(node);
    }
    if (gpu_nodes.empty()) {
        error = "visible GPU PCI sysfs discovery produced no online NUMA node";
        return false;
    }
    selection.numa_nodes.assign(gpu_nodes.begin(), gpu_nodes.end());
    selection.source = "visible GPU PCI sysfs";
    return true;
}

class ScopedCudaDevice {
   public:
    explicit ScopedCudaDevice(int device) {
        result_ = cudaGetDevice(&previous_device_);
        if (result_ != cudaSuccess) return;
        result_ = cudaSetDevice(device);
        restore_ = result_ == cudaSuccess && previous_device_ != device;
    }

    ScopedCudaDevice(const ScopedCudaDevice&) = delete;
    ScopedCudaDevice& operator=(const ScopedCudaDevice&) = delete;

    ~ScopedCudaDevice() {
        if (restore_) (void)cudaSetDevice(previous_device_);
    }

    cudaError_t result() const { return result_; }

   private:
    int previous_device_ = 0;
    cudaError_t result_ = cudaSuccess;
    bool restore_ = false;
};

class CudaBuffer {
   public:
    explicit CudaBuffer(int device) : device_(device) {}
    CudaBuffer(const CudaBuffer&) = delete;
    CudaBuffer& operator=(const CudaBuffer&) = delete;
    ~CudaBuffer() { (void)Reset(); }

    cudaError_t Allocate(size_t length) {
        ScopedCudaDevice current(device_);
        if (current.result() != cudaSuccess) return current.result();
        return cudaMalloc(&address_, length);
    }

    cudaError_t Reset() {
        if (address_ == nullptr) return cudaSuccess;
        ScopedCudaDevice current(device_);
        if (current.result() != cudaSuccess) return current.result();
        cudaError_t result = cudaFree(address_);
        if (result == cudaSuccess) address_ = nullptr;
        return result;
    }

    void* get() const { return address_; }

   private:
    int device_;
    void* address_ = nullptr;
};

class RegisteredMemory {
   public:
    RegisteredMemory(TransferEngine& engine, void* address)
        : engine_(engine), address_(address) {}
    RegisteredMemory(const RegisteredMemory&) = delete;
    RegisteredMemory& operator=(const RegisteredMemory&) = delete;
    ~RegisteredMemory() { (void)Reset(); }

    int Register(size_t length, const std::string& location,
                 bool remote_accessible) {
        const int result = engine_.registerLocalMemory(
            address_, length, location, remote_accessible);
        active_ = result == 0;
        return result;
    }

    int Reset() {
        if (!active_) return 0;
        const int result = engine_.unregisterLocalMemory(address_);
        if (result == 0) active_ = false;
        return result;
    }

   private:
    TransferEngine& engine_;
    void* address_;
    bool active_ = false;
};

class ScopedBatch {
   public:
    explicit ScopedBatch(TransferEngine& engine)
        : engine_(engine), id_(engine.allocateBatchID(1)) {
        if (id_ == static_cast<BatchID>(ERR_MEMORY)) id_ = INVALID_BATCH_ID;
    }
    ScopedBatch(const ScopedBatch&) = delete;
    ScopedBatch& operator=(const ScopedBatch&) = delete;
    ~ScopedBatch() { (void)Reset(); }

    BatchID id() const { return id_; }

    Status Reset() {
        if (id_ == INVALID_BATCH_ID) return Status::OK();
        Status status = engine_.freeBatchID(id_);
        if (status.ok()) id_ = INVALID_BATCH_ID;
        return status;
    }

    Status ForceReset() {
        if (id_ == INVALID_BATCH_ID) return Status::OK();
        // Once device work is quiescent, forcing every task terminal makes a
        // batch releasable even if status polling itself failed.
        (void)cudaDeviceSynchronize();
        auto& desc = Transport::toBatchDesc(id_);
        for (auto& task : desc.task_list) Transport::markSubmissionFailed(task);
        return Reset();
    }

   private:
    TransferEngine& engine_;
    BatchID id_ = INVALID_BATCH_ID;
};

std::vector<unsigned char> MakePattern(size_t length, unsigned int seed) {
    std::vector<unsigned char> pattern(length);
    for (size_t index = 0; index < length; ++index) {
        pattern[index] = static_cast<unsigned char>(
            (seed + index * 17U + (index >> 8U) * 29U) & 0xffU);
    }
    return pattern;
}

testing::AssertionResult MatchesPattern(
    const unsigned char* actual, const std::vector<unsigned char>& expected) {
    for (size_t index = 0; index < expected.size(); ++index) {
        if (actual[index] != expected[index]) {
            return testing::AssertionFailure()
                   << "byte mismatch at offset " << index << ": expected "
                   << static_cast<unsigned int>(expected[index]) << ", got "
                   << static_cast<unsigned int>(actual[index]);
        }
    }
    return testing::AssertionSuccess();
}

std::string RunTransfer(TransferEngine& engine, TransferRequest::OpCode opcode,
                        void* local_address, SegmentID target_id,
                        void* remote_address, size_t length) {
    ScopedBatch batch(engine);
    if (batch.id() == INVALID_BATCH_ID) return "batch allocation failed";

    TransferRequest request;
    request.opcode = opcode;
    request.source = local_address;
    request.target_id = target_id;
    request.target_offset = reinterpret_cast<uint64_t>(remote_address);
    request.length = length;

    Status status = engine.submitTransfer(batch.id(), {request});
    if (!status.ok()) {
        const std::string error = "submitTransfer failed: " + status.ToString();
        Status cleanup = batch.ForceReset();
        return cleanup.ok()
                   ? error
                   : error + "; batch cleanup failed: " + cleanup.ToString();
    }

    const auto deadline = std::chrono::steady_clock::now() + kTransferTimeout;
    TransferStatus transfer_status{};
    for (;;) {
        status = engine.getTransferStatus(batch.id(), 0, transfer_status);
        if (!status.ok()) {
            const std::string error =
                "getTransferStatus failed: " + status.ToString();
            Status cleanup = batch.ForceReset();
            return cleanup.ok()
                       ? error
                       : error +
                             "; batch cleanup failed: " + cleanup.ToString();
        }
        if (transfer_status.s == TransferStatusEnum::COMPLETED) break;
        if (transfer_status.s == TransferStatusEnum::FAILED) {
            Status cleanup = batch.Reset();
            return cleanup.ok() ? "transfer completed with FAILED status"
                                : "transfer failed and batch cleanup failed: " +
                                      cleanup.ToString();
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            // Synchronize before marking the task terminal so no in-flight
            // copy can retain Slice ownership after the batch is released.
            const cudaError_t sync_result = cudaDeviceSynchronize();
            status = engine.getTransferStatus(batch.id(), 0, transfer_status);
            if (status.ok() &&
                transfer_status.s == TransferStatusEnum::COMPLETED) {
                break;
            }
            Status cleanup = batch.ForceReset();
            std::ostringstream error;
            error << "transfer timed out; cudaDeviceSynchronize result="
                  << static_cast<int>(sync_result);
            if (!cleanup.ok())
                error << "; batch cleanup failed: " << cleanup.ToString();
            return error.str();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (transfer_status.transferred_bytes != length) {
        std::ostringstream error;
        error << "transferred byte count mismatch: expected " << length
              << ", got " << transfer_status.transferred_bytes;
        return error.str();
    }
    status = batch.Reset();
    return status.ok() ? std::string()
                       : "freeBatchID failed: " + status.ToString();
}

#define REQUIRE_FABRIC_OR_SKIP(strict, reason) \
    do {                                       \
        if (strict) {                          \
            FAIL() << (reason);                \
        } else {                               \
            GTEST_SKIP() << (reason);          \
        }                                      \
    } while (false)

TEST(NvlinkVmmFabricTest, CpuAllVisibleGpusAndColdWarmFabricPath) {
    bool strict = false;
    std::string prerequisite_error;
    ASSERT_TRUE(
        ParseStrictMode("MC_REQUIRE_MNNVL_FABRIC", strict, prerequisite_error))
        << prerequisite_error;

    HardwareSelection hardware;
    if (!SelectHardware(hardware, prerequisite_error)) {
        REQUIRE_FABRIC_OR_SKIP(strict, prerequisite_error);
    }

    Status capability = NvlinkVmmAllocation::CheckStrictFabricCapability();
    if (!capability.ok()) {
        REQUIRE_FABRIC_OR_SKIP(
            strict,
            "Fabric capability prerequisite failed: " + capability.ToString());
    }

    ASSERT_FALSE(hardware.numa_nodes.empty());
    std::ostringstream selected_nodes;
    for (size_t index = 0; index < hardware.numa_nodes.size(); ++index) {
        if (index != 0) selected_nodes << ',';
        selected_nodes << hardware.numa_nodes[index];
    }
    ::testing::Test::RecordProperty("host_numa_nodes", selected_nodes.str());
    ::testing::Test::RecordProperty("visible_gpu_count",
                                    std::to_string(hardware.devices.size()));
    ::testing::Test::RecordProperty("numa_selection_source", hardware.source);

    for (int numa_node : hardware.numa_nodes) {
        SCOPED_TRACE("HOST_NUMA node " + std::to_string(numa_node));

        NvlinkVmmAllocation::Options options;
        options.location_type = NvlinkVmmAllocation::LocationType::HOST_NUMA;
        options.location_id = numa_node;
        options.requested_length = kTransferBytes;
        options.fabric_exportable = true;

        auto server = std::make_unique<TransferEngine>(false);
        ASSERT_EQ(server->init(P2PHANDSHAKE, "127.0.0.1:0", "127.0.0.1", 0), 0);
        ASSERT_NE(server->installTransport("nvlink", nullptr), nullptr);

        std::unique_ptr<NvlinkVmmAllocation> allocation;
        Status allocation_status =
            NvlinkVmmAllocation::Create(options, allocation);
        if (!allocation_status.ok()) {
            REQUIRE_FABRIC_OR_SKIP(
                strict,
                "HOST_NUMA Fabric VMM allocation prerequisite failed on " +
                    hardware.source + ": " + allocation_status.ToString());
        }
        ASSERT_NE(allocation, nullptr);
        ASSERT_NE(allocation->base(), nullptr);
        ASSERT_GE(allocation->length(), kTransferBytes);
        ASSERT_EQ(allocation->location_type(),
                  NvlinkVmmAllocation::LocationType::HOST_NUMA);
        ASSERT_EQ(allocation->location_id(), numa_node);
        ASSERT_TRUE(allocation->fabric_exportable());

        auto* host_numa = static_cast<unsigned char*>(allocation->base());
        const auto cpu_pattern = MakePattern(kTransferBytes, 0x21U);
        std::copy(cpu_pattern.begin(), cpu_pattern.end(), host_numa);
        ASSERT_TRUE(MatchesPattern(host_numa, cpu_pattern));

        RegisteredMemory server_registration(*server, allocation->base());
        const int registration_result = server_registration.Register(
            allocation->length(), "cpu:numa" + std::to_string(numa_node), true);
        if (registration_result != 0) {
            REQUIRE_FABRIC_OR_SKIP(strict,
                                   "Fabric export/IMEX prerequisite failed "
                                   "during NVLink registration"
                                   " (rc=" +
                                       std::to_string(registration_result) +
                                       ")");
        }

        auto local_desc =
            server->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID, false);
        ASSERT_NE(local_desc, nullptr);
        bool exported = false;
        for (const auto& buffer : local_desc->buffers) {
            if (buffer.addr == reinterpret_cast<uint64_t>(allocation->base()) &&
                buffer.length == allocation->length()) {
                exported = !buffer.shm_name.empty();
                break;
            }
        }
        ASSERT_TRUE(exported)
            << "remote registration must publish one serialized Fabric handle";

        auto client = std::make_unique<TransferEngine>(false);
        ASSERT_EQ(client->init(P2PHANDSHAKE, "127.0.0.1:0", "127.0.0.1", 0), 0);
        ASSERT_NE(client->installTransport("nvlink", nullptr), nullptr);
        auto* client_nvlink =
            dynamic_cast<NvlinkTransport*>(client->getTransport("nvlink"));
        ASSERT_NE(client_nvlink, nullptr);
        auto import_audit = std::make_shared<FabricImportAudit>();
        NvlinkTransportTestPeer::InstallOneShotMapFailure(*client_nvlink,
                                                          import_audit);
        const SegmentID provider_segment =
            client->openSegment(server->getLocalIpAndPort());
        ASSERT_NE(provider_segment, static_cast<SegmentID>(-1));

        bool cold_transfer_completed = false;
        for (int device : hardware.devices) {
            SCOPED_TRACE("visible CUDA device " + std::to_string(device));
            ScopedCudaDevice current(device);
            ASSERT_EQ(current.result(), cudaSuccess)
                << cudaGetErrorString(current.result());

            CudaBuffer hbm(device);
            ASSERT_EQ(hbm.Allocate(kTransferBytes), cudaSuccess);
            ASSERT_NE(hbm.get(), nullptr);

            RegisteredMemory client_registration(*client, hbm.get());
            ASSERT_EQ(
                client_registration.Register(
                    kTransferBytes, "cuda:" + std::to_string(device), false),
                0);

            // Direct copies prove that the owning CPU and every visible GPU
            // have read/write access to the HOST_NUMA mapping.
            const auto direct_to_hbm =
                MakePattern(kTransferBytes, 0x40U + device);
            std::copy(direct_to_hbm.begin(), direct_to_hbm.end(), host_numa);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            ASSERT_EQ(cudaMemcpy(hbm.get(), host_numa, kTransferBytes,
                                 cudaMemcpyDefault),
                      cudaSuccess);
            std::vector<unsigned char> observed(kTransferBytes);
            ASSERT_EQ(cudaMemcpy(observed.data(), hbm.get(), kTransferBytes,
                                 cudaMemcpyDeviceToHost),
                      cudaSuccess);
            ASSERT_TRUE(MatchesPattern(observed.data(), direct_to_hbm));

            const auto direct_to_dram =
                MakePattern(kTransferBytes, 0x60U + device);
            ASSERT_EQ(cudaMemcpy(hbm.get(), direct_to_dram.data(),
                                 kTransferBytes, cudaMemcpyHostToDevice),
                      cudaSuccess);
            ASSERT_EQ(cudaMemcpy(host_numa, hbm.get(), kTransferBytes,
                                 cudaMemcpyDefault),
                      cudaSuccess);
            ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
            ASSERT_TRUE(MatchesPattern(host_numa, direct_to_dram));

            const auto write_pattern =
                MakePattern(kTransferBytes, 0x80U + device);
            ASSERT_EQ(cudaMemcpy(hbm.get(), write_pattern.data(),
                                 kTransferBytes, cudaMemcpyHostToDevice),
                      cudaSuccess);
            if (!cold_transfer_completed) {
                const std::string injected_error = RunTransfer(
                    *client, TransferRequest::WRITE, hbm.get(),
                    provider_segment, allocation->base(), kTransferBytes);
                ASSERT_FALSE(injected_error.empty())
                    << "one-shot cuMemMap failure was not surfaced";
                if (import_audit->injected_map_failures.load() == 0) {
                    REQUIRE_FABRIC_OR_SKIP(
                        strict,
                        "cold Fabric import prerequisite failed before "
                        "the injected cuMemMap stage: " +
                            injected_error);
                }
                ASSERT_EQ(import_audit->injected_map_failures.load(), 1);
                ASSERT_EQ(NvlinkTransportTestPeer::MappingCount(*client_nvlink),
                          0U);
                ASSERT_EQ(import_audit->live_handles.load(), 0);
                ASSERT_EQ(import_audit->live_reserved_ranges.load(), 0);
                ASSERT_EQ(import_audit->live_mappings.load(), 0);
            }

            const int imports_before_success =
                import_audit->import_calls.load();
            const std::string write_error = RunTransfer(
                *client, TransferRequest::WRITE, hbm.get(), provider_segment,
                allocation->base(), kTransferBytes);
            if (!cold_transfer_completed && !write_error.empty()) {
                REQUIRE_FABRIC_OR_SKIP(
                    strict,
                    "Fabric import retry prerequisite failed after clean "
                    "rollback: " +
                        write_error);
            }
            ASSERT_TRUE(write_error.empty()) << write_error;
            if (!cold_transfer_completed) {
                ASSERT_EQ(import_audit->import_calls.load(),
                          imports_before_success + 1);
                ASSERT_EQ(NvlinkTransportTestPeer::MappingCount(*client_nvlink),
                          1U);
                ASSERT_EQ(import_audit->live_handles.load(), 0);
                ASSERT_EQ(import_audit->live_reserved_ranges.load(), 1);
                ASSERT_EQ(import_audit->live_mappings.load(), 1);
            }
            cold_transfer_completed = true;
            std::atomic_thread_fence(std::memory_order_seq_cst);
            ASSERT_TRUE(MatchesPattern(host_numa, write_pattern));

            // The next request uses the same Consumer engine, segment, and
            // remote BufferDesc. It therefore exercises the warm mapping-cache
            // path.
            const auto read_pattern =
                MakePattern(kTransferBytes, 0xa0U + device);
            std::copy(read_pattern.begin(), read_pattern.end(), host_numa);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            const int imports_before_warm = import_audit->import_calls.load();
            const std::string read_error = RunTransfer(
                *client, TransferRequest::READ, hbm.get(), provider_segment,
                allocation->base(), kTransferBytes);
            ASSERT_TRUE(read_error.empty()) << read_error;
            ASSERT_EQ(import_audit->import_calls.load(), imports_before_warm)
                << "warm transfer unexpectedly missed the Fabric map cache";
            ASSERT_EQ(cudaMemcpy(observed.data(), hbm.get(), kTransferBytes,
                                 cudaMemcpyDeviceToHost),
                      cudaSuccess);
            ASSERT_TRUE(MatchesPattern(observed.data(), read_pattern));

            ASSERT_EQ(client_registration.Reset(), 0);
            ASSERT_EQ(hbm.Reset(), cudaSuccess);
        }
        ASSERT_TRUE(cold_transfer_completed);

        // Destroy the Consumer first so imported mappings are unmapped before
        // the Provider unregisters and releases the owning VMM allocation.
        client.reset();
        ASSERT_EQ(import_audit->live_handles.load(), 0);
        ASSERT_EQ(import_audit->live_reserved_ranges.load(), 0);
        ASSERT_EQ(import_audit->live_mappings.load(), 0);
        ASSERT_EQ(server_registration.Reset(), 0);
        allocation.reset();
        server.reset();
    }
}

#undef REQUIRE_FABRIC_OR_SKIP

}  // namespace
}  // namespace mooncake

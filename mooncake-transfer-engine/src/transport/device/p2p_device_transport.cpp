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

// P2P device transport — unified NVLink (CUDA) + MTLink (MUSA) implementation.
//
// Uses cuda_alike.h so all cuda* APIs map to musa* when USE_MUSA is defined.
// No #ifdef USE_MUSA / MOONCAKE_EP_USE_MUSA in this file.

#include "transport/device/device_transport.h"

#include <glog/logging.h>
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

#include "cuda_alike.h"

namespace mooncake {
namespace device {

namespace {

#if defined(USE_CUDA)
bool supportFabricMem() {
    const char* nvlink_ipc = std::getenv("MC_USE_NVLINK_IPC");
    if (nvlink_ipc == nullptr || std::strcmp(nvlink_ipc, "0") != 0)
        return false;

    int num_devices = 0;
    cudaError_t err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess || num_devices == 0) return false;

    for (int device_id = 0; device_id < num_devices; ++device_id) {
        CUdevice cu_device;
        if (cuDeviceGet(&cu_device, device_id) != CUDA_SUCCESS) return false;
        int supported = 0;
        if (cuDeviceGetAttribute(
                &supported, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
                cu_device) != CUDA_SUCCESS ||
            !supported)
            return false;
    }
    return true;
}

std::vector<int32_t> serializeFabricHandle(const CUmemFabricHandle& handle,
                                           size_t bytes) {
    constexpr size_t kPayloadBytes =
        sizeof(CUmemFabricHandle) + sizeof(uint64_t);
    constexpr size_t kNumInt32s =
        (kPayloadBytes + sizeof(int32_t) - 1) / sizeof(int32_t);
    std::vector<int32_t> result(kNumInt32s, 0);
    auto* payload = reinterpret_cast<uint8_t*>(result.data());
    uint64_t encoded_size = static_cast<uint64_t>(bytes);
    memcpy(payload, &handle, sizeof(CUmemFabricHandle));
    memcpy(payload + sizeof(CUmemFabricHandle), &encoded_size,
           sizeof(encoded_size));
    return result;
}

bool deserializeFabricHandle(const std::vector<int32_t>& encoded,
                             CUmemFabricHandle* handle, size_t* bytes) {
    constexpr size_t kPayloadBytes =
        sizeof(CUmemFabricHandle) + sizeof(uint64_t);
    constexpr size_t kNumInt32s =
        (kPayloadBytes + sizeof(int32_t) - 1) / sizeof(int32_t);
    if (encoded.size() < kNumInt32s) return false;
    const auto* payload = reinterpret_cast<const uint8_t*>(encoded.data());
    uint64_t encoded_size = 0;
    memcpy(handle, payload, sizeof(CUmemFabricHandle));
    memcpy(&encoded_size, payload + sizeof(CUmemFabricHandle),
           sizeof(encoded_size));
    *bytes = static_cast<size_t>(encoded_size);
    return *bytes != 0;
}

struct FabricAllocation {
    CUdeviceptr ptr = 0;
    size_t size = 0;
    CUmemGenericAllocationHandle handle{};
};
#else
bool supportFabricMem() { return false; }
#endif

}  // namespace

#ifdef USE_MACA
namespace {

bool parseBoolEnv(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr) return false;
    std::string s(value);
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return s == "1" || s == "on" || s == "true" || s == "yes";
}

std::string getLowerEnv(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr) return "";
    std::string s(value);
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return s;
}

int macaAllocFlagFromMode(const std::string& mode, const char* env_name) {
    if (mode.empty() || mode == "default" || mode == "cuda")
        return mcDeviceMallocDefault;
    if (mode == "fine" || mode == "finegrained" || mode == "fine-grained")
        return mcDeviceMallocFinegrained;
    if (mode == "signal") return mcMallocSignalMemory;
    if (mode == "wc" || mode == "writecoherence" || mode == "write-coherence")
        return mcDeviceMallocWriteCoherence;
    if (mode == "pcie" || mode == "pcie-uncache" || mode == "map-pcie")
        return mcDeviceMallocMapPcieDefault;
    if (mode == "pcie-wc" || mode == "map-pcie-wc")
        return mcDeviceMallocMapPcieCoherence;
    if (mode == "fixed" || mode == "fixed-uncache")
        return mcDeviceMallocFixedMemDefault;
    if (mode == "fixed-wc") return mcDeviceMallocFixedMemCoherence;
    LOG(WARNING) << "[EP P2P] unknown " << env_name << "=" << mode
                 << ", using default cudaMalloc";
    return mcDeviceMallocDefault;
}

int macaAllocFlagFromEnv() {
    return macaAllocFlagFromMode(getLowerEnv("MOONCAKE_EP_MACA_ALLOC"),
                                 "MOONCAKE_EP_MACA_ALLOC");
}

std::string macaIpcMode() {
    std::string mode = getLowerEnv("MOONCAKE_EP_MACA_IPC");
    return mode.empty() ? "normal" : mode;
}

bool parseNonNegativeInt(const std::string& token, int* value) {
    if (token.empty()) return false;
    int result = 0;
    for (char c : token) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
        result = result * 10 + (c - '0');
    }
    *value = result;
    return true;
}

int physicalDeviceFromVisibleList(int logical_device) {
    const char* visible = std::getenv("CUDA_VISIBLE_DEVICES");
    if (visible == nullptr || visible[0] == '\0') return logical_device;

    std::string list(visible);
    size_t begin = 0;
    int logical = 0;
    while (begin <= list.size()) {
        size_t end = list.find(',', begin);
        if (end == std::string::npos) end = list.size();
        std::string token = list.substr(begin, end - begin);
        token.erase(std::remove_if(
                        token.begin(), token.end(),
                        [](unsigned char c) { return std::isspace(c) != 0; }),
                    token.end());
        if (logical == logical_device) {
            int physical = logical_device;
            return parseNonNegativeInt(token, &physical) ? physical
                                                         : logical_device;
        }
        if (end == list.size()) break;
        begin = end + 1;
        ++logical;
    }
    return logical_device;
}

bool pairListed(const std::string& pairs, int src, int dst) {
    size_t begin = 0;
    while (begin <= pairs.size()) {
        size_t end = pairs.find(',', begin);
        if (end == std::string::npos) end = pairs.size();
        std::string item = pairs.substr(begin, end - begin);
        item.erase(std::remove_if(
                       item.begin(), item.end(),
                       [](unsigned char c) { return std::isspace(c) != 0; }),
                   item.end());

        size_t dash = item.find('-');
        if (dash != std::string::npos) {
            int a = -1, b = -1;
            if (parseNonNegativeInt(item.substr(0, dash), &a) &&
                parseNonNegativeInt(item.substr(dash + 1), &b)) {
                if ((a == src && b == dst) || (a == dst && b == src))
                    return true;
            }
        }

        if (end == pairs.size()) break;
        begin = end + 1;
    }
    return false;
}

bool macaP2pPairAllowed(int src_physical, int dst_physical) {
    if (parseBoolEnv("MOONCAKE_EP_MACA_ALLOW_NODE_P2P")) return true;

    const char* explicit_pairs = std::getenv("MOONCAKE_EP_MACA_P2P_PAIRS");
    if (explicit_pairs != nullptr && explicit_pairs[0] != '\0')
        return pairListed(explicit_pairs, src_physical, dst_physical);

    // C500 exposes two direct MetaXLink islands by default: 0<->1 and 2<->3.
    // NODE pairs may report canAccessPeer=1, but EP kernel peer stores can hang
    // waiting for device-side signals on those paths.
    return src_physical / 2 == dst_physical / 2 &&
           std::abs(src_physical - dst_physical) == 1;
}

}  // namespace
#endif

class P2pDeviceTransportImpl : public P2pTransport {
   public:
    explicit P2pDeviceTransportImpl(int num_ranks)
        : num_ranks_(num_ranks), use_fabric_mem_(supportFabricMem()) {
        cudaMalloc(&available_table_, num_ranks_ * sizeof(int32_t));
        cudaMemset(available_table_, 0, num_ranks_ * sizeof(int32_t));
        cudaMallocHost(&peer_ptrs_host_, num_ranks_ * sizeof(void*));
        cudaMalloc(&peer_ptrs_dev_, num_ranks_ * sizeof(void*));
        for (int i = 0; i < num_ranks_; ++i) peer_ptrs_host_[i] = nullptr;
        cudaMemset(peer_ptrs_dev_, 0, num_ranks_ * sizeof(void*));
#if defined(USE_CUDA)
        fabric_peer_mappings_.resize(num_ranks_);
#endif
    }

    ~P2pDeviceTransportImpl() override {
#if defined(USE_CUDA)
        cleanupFabricPeerMappings();
        cleanupFabricAllocations();
#endif
        if (available_table_) cudaFree(available_table_);
        if (peer_ptrs_dev_) cudaFree(peer_ptrs_dev_);
        if (peer_ptrs_host_) {
            for (int i = 0; i < num_ranks_ && !use_fabric_mem_; ++i) {
                if (peer_ptrs_host_[i] && peer_ptrs_host_[i] != local_ptr_) {
                    cudaIpcCloseMemHandle(peer_ptrs_host_[i]);
                }
            }
            cudaFreeHost(peer_ptrs_host_);
        }
    }

    void* allocateBuffer(size_t bytes) override {
        void* ptr = nullptr;
#if defined(USE_CUDA)
        if (use_fabric_mem_) {
            int device_id = 0;
            if (cudaGetDevice(&device_id) != cudaSuccess) {
                LOG(ERROR)
                    << "[EP P2P] cudaGetDevice failed before fabric alloc";
                return nullptr;
            }

            CUdevice cu_dev;
            CUresult res = cuDeviceGet(&cu_dev, device_id);
            if (res != CUDA_SUCCESS) {
                LOG(ERROR) << "[EP P2P] cuDeviceGet failed: " << res;
                return nullptr;
            }

            CUmemAllocationProp prop = {};
            prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
            prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
            prop.location.id = cu_dev;
            prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;

            int rdma_capable = 0;
            cuDeviceGetAttribute(
                &rdma_capable,
                CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
                cu_dev);
            if (rdma_capable) prop.allocFlags.gpuDirectRDMACapable = 1;

            size_t granularity = 0;
            res = cuMemGetAllocationGranularity(
                &granularity, &prop, CU_MEM_ALLOC_GRANULARITY_MINIMUM);
            if (res != CUDA_SUCCESS) {
                LOG(ERROR) << "[EP P2P] cuMemGetAllocationGranularity failed: "
                           << res;
                return nullptr;
            }

            size_t fabric_alloc_size = std::max(
                granularity, (bytes + granularity - 1) & ~(granularity - 1));
            CUmemGenericAllocationHandle fabric_mem_handle{};
            res = cuMemCreate(&fabric_mem_handle, fabric_alloc_size, &prop, 0);
            if (res != CUDA_SUCCESS) {
                LOG(ERROR) << "[EP P2P] cuMemCreate(FABRIC) failed: " << res;
                return nullptr;
            }

            CUdeviceptr reserved = 0;
            res = cuMemAddressReserve(&reserved, fabric_alloc_size, granularity,
                                      0, 0);
            if (res != CUDA_SUCCESS) {
                cuMemRelease(fabric_mem_handle);
                LOG(ERROR) << "[EP P2P] cuMemAddressReserve failed: " << res;
                return nullptr;
            }

            res =
                cuMemMap(reserved, fabric_alloc_size, 0, fabric_mem_handle, 0);
            if (res != CUDA_SUCCESS) {
                cuMemAddressFree(reserved, fabric_alloc_size);
                cuMemRelease(fabric_mem_handle);
                LOG(ERROR) << "[EP P2P] cuMemMap failed: " << res;
                return nullptr;
            }

            int device_count = 0;
            cudaGetDeviceCount(&device_count);
            std::vector<CUmemAccessDesc> access(device_count);
            for (int i = 0; i < device_count; ++i) {
                access[i].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
                access[i].location.id = i;
                access[i].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
            }
            res = cuMemSetAccess(reserved, fabric_alloc_size, access.data(),
                                 device_count);
            if (res != CUDA_SUCCESS) {
                cuMemUnmap(reserved, fabric_alloc_size);
                cuMemAddressFree(reserved, fabric_alloc_size);
                cuMemRelease(fabric_mem_handle);
                LOG(ERROR) << "[EP P2P] cuMemSetAccess failed: " << res;
                return nullptr;
            }

            ptr = reinterpret_cast<void*>(reserved);
            fabric_allocations_.emplace(
                ptr, FabricAllocation{reserved, fabric_alloc_size,
                                      fabric_mem_handle});
            LOG(INFO) << "[EP P2P] allocated fabric buffer bytes=" << bytes
                      << " mapped_bytes=" << fabric_alloc_size;
            return ptr;
        }
#endif
#ifdef USE_MACA
        int alloc_flag = macaAllocFlagFromEnv();
        cudaError_t err = alloc_flag == mcDeviceMallocDefault
                              ? cudaMalloc(&ptr, bytes)
                              : mcExtMallocWithFlags(&ptr, bytes, alloc_flag);
#else
        cudaError_t err = cudaMalloc(&ptr, bytes);
#endif
        if (err != cudaSuccess) {
            LOG(ERROR) << "[EP P2P] device allocation(" << bytes
                       << ") failed: " << cudaGetErrorString(err);
            return nullptr;
        }
#ifdef USE_MACA
        if (alloc_flag != mcDeviceMallocDefault) {
            LOG(INFO) << "[EP P2P] allocated MACA buffer with "
                         "mcExtMallocWithFlags flag="
                      << alloc_flag;
        }
#endif
        return ptr;
    }

    void freeBuffer(void* ptr) override {
#if defined(USE_CUDA)
        if (ptr == nullptr) return;
        if (use_fabric_mem_) {
            auto it = fabric_allocations_.find(ptr);
            if (it == fabric_allocations_.end()) {
                LOG(ERROR) << "[EP P2P] unknown fabric buffer=" << ptr;
                return;
            }
            if (ptr == local_ptr_) {
                cleanupFabricPeerMappings();
                local_ptr_ = nullptr;
            }
            FabricAllocation allocation = it->second;
            fabric_allocations_.erase(it);
            cuMemUnmap(allocation.ptr, allocation.size);
            cuMemAddressFree(allocation.ptr, allocation.size);
            cuMemRelease(allocation.handle);
            return;
        }
#endif
        cudaFree(ptr);
    }

    std::vector<int32_t> exportIpcHandle(void* ptr) override {
#if defined(USE_CUDA)
        if (use_fabric_mem_) {
            auto it = fabric_allocations_.find(ptr);
            if (it == fabric_allocations_.end()) {
                LOG(ERROR) << "[EP P2P] unknown fabric buffer=" << ptr;
                return {};
            }
            CUmemFabricHandle export_handle;
            CUresult res =
                cuMemExportToShareableHandle(&export_handle, it->second.handle,
                                             CU_MEM_HANDLE_TYPE_FABRIC, 0);
            if (res != CUDA_SUCCESS) {
                LOG(ERROR)
                    << "[EP P2P] cuMemExportToShareableHandle(FABRIC) failed: "
                    << res;
                return {};
            }
            return serializeFabricHandle(export_handle, it->second.size);
        }
#endif
#ifdef USE_MACA
        if (parseBoolEnv("MOONCAKE_EP_MACA_DISABLE_IPC")) {
            LOG(INFO) << "[EP P2P] MACA IPC handle export disabled by "
                         "MOONCAKE_EP_MACA_DISABLE_IPC";
            return {};
        }

        cudaPointerAttributes attr{};
        cudaError_t attr_err = cudaPointerGetAttributes(&attr, ptr);
        if (attr_err != cudaSuccess || attr.type != cudaMemoryTypeDevice ||
            attr.devicePointer == nullptr) {
            LOG(WARNING) << "[EP P2P] skip MACA IPC handle export for "
                         << "non-device pointer=" << ptr
                         << ", attr_err=" << cudaGetErrorString(attr_err)
                         << ", type=" << attr.type
                         << ", devicePointer=" << attr.devicePointer
                         << ", allocationFlags=" << attr.allocationFlags;
            return {};
        }

        std::string ipc_mode = macaIpcMode();
        if (ipc_mode == "cross-v2" || ipc_mode == "cross_v2") {
            mcIpcCrossMemHandle_t handle;
            cudaError_t err = mcIpcGetMemHandleCross_v2(&handle, ptr);
            if (err != cudaSuccess) {
                LOG(ERROR) << "[EP P2P] mcIpcGetMemHandleCross_v2 failed: "
                           << cudaGetErrorString(err);
                return {};
            }
            constexpr size_t kHandleBytes = sizeof(mcIpcCrossMemHandle_t);
            constexpr size_t kNumInt32s =
                (kHandleBytes + sizeof(int32_t) - 1) / sizeof(int32_t);
            std::vector<int32_t> result(kNumInt32s);
            memcpy(result.data(), &handle, kHandleBytes);
            return result;
        }
#endif
        cudaIpcMemHandle_t handle;
#ifdef USE_MACA
        cudaError_t err = macaIpcMode() == "cross"
                              ? mcIpcGetMemHandleCross(&handle, ptr)
                              : cudaIpcGetMemHandle(&handle, ptr);
#else
        cudaError_t err = cudaIpcGetMemHandle(&handle, ptr);
#endif
        if (err != cudaSuccess) {
            LOG(ERROR) << "[EP P2P] IPC handle export failed: "
                       << cudaGetErrorString(err);
            return {};
        }
        constexpr size_t kHandleBytes = sizeof(cudaIpcMemHandle_t);
        constexpr size_t kNumInt32s =
            (kHandleBytes + sizeof(int32_t) - 1) / sizeof(int32_t);
        std::vector<int32_t> result(kNumInt32s);
        memcpy(result.data(), &handle, kHandleBytes);
        return result;
    }

    void importPeerHandles(
        void* local_ptr, int rank, int num_ranks,
        const std::vector<std::vector<int32_t>>& remote_handles,
        const std::vector<int>& active_ranks_mask) override {
        local_ptr_ = local_ptr;
        int device_id = 0;
        cudaGetDevice(&device_id);
        int device_count = 0;
        cudaGetDeviceCount(&device_count);
        CHECK_GT(device_count, 0) << "No CUDA/MUSA devices found";

        std::vector<int32_t> available(num_ranks_, 0);
        available[rank] = 1;
        peer_ptrs_host_[rank] = local_ptr;

#if defined(USE_CUDA)
        if (use_fabric_mem_) {
            cleanupFabricPeerMappings();
            all_peers_accessible_ = true;
            std::vector<CUmemAccessDesc> access(device_count);
            for (int i = 0; i < device_count; ++i) {
                access[i].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
                access[i].location.id = i;
                access[i].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
            }
            for (int dst = 0; dst < num_ranks_; ++dst) {
                if (active_ranks_mask[dst] == 0) continue;
                if (dst == rank) continue;
                if (dst >= static_cast<int>(remote_handles.size())) {
                    all_peers_accessible_ = false;
                    break;
                }
                CUmemFabricHandle export_handle;
                size_t mapped_size = 0;
                if (!deserializeFabricHandle(remote_handles[dst],
                                             &export_handle, &mapped_size)) {
                    all_peers_accessible_ = false;
                    break;
                }

                CUmemGenericAllocationHandle handle;
                CUresult res = cuMemImportFromShareableHandle(
                    &handle, &export_handle, CU_MEM_HANDLE_TYPE_FABRIC);
                if (res != CUDA_SUCCESS) {
                    LOG(ERROR)
                        << "[EP P2P] cuMemImportFromShareableHandle(FABRIC) "
                           "failed for rank "
                        << dst << ": " << res;
                    all_peers_accessible_ = false;
                    break;
                }

                CUdeviceptr peer_reserved = 0;
                res = cuMemAddressReserve(&peer_reserved, mapped_size, 0, 0, 0);
                if (res != CUDA_SUCCESS) {
                    cuMemRelease(handle);
                    LOG(ERROR)
                        << "[EP P2P] cuMemAddressReserve peer fabric mapping "
                           "failed for rank "
                        << dst << ": " << res;
                    all_peers_accessible_ = false;
                    break;
                }
                res = cuMemMap(peer_reserved, mapped_size, 0, handle, 0);
                if (res != CUDA_SUCCESS) {
                    cuMemAddressFree(peer_reserved, mapped_size);
                    cuMemRelease(handle);
                    LOG(ERROR) << "[EP P2P] cuMemMap peer fabric mapping "
                                  "failed for rank "
                               << dst << ": " << res;
                    all_peers_accessible_ = false;
                    break;
                }

                res = cuMemSetAccess(peer_reserved, mapped_size, access.data(),
                                     device_count);
                if (res != CUDA_SUCCESS) {
                    cuMemUnmap(peer_reserved, mapped_size);
                    cuMemAddressFree(peer_reserved, mapped_size);
                    cuMemRelease(handle);
                    LOG(ERROR) << "[EP P2P] cuMemSetAccess peer fabric mapping "
                                  "failed for rank "
                               << dst << ": " << res;
                    all_peers_accessible_ = false;
                    break;
                }

                void* peer_ptr = reinterpret_cast<void*>(peer_reserved);
                fabric_peer_mappings_[dst] = {peer_ptr, mapped_size, handle};
                available[dst] = 1;
                peer_ptrs_host_[dst] = peer_ptr;
            }
            cudaMemcpy(available_table_, available.data(),
                       num_ranks_ * sizeof(int32_t), cudaMemcpyHostToDevice);
            cudaMemcpy(peer_ptrs_dev_, peer_ptrs_host_,
                       num_ranks_ * sizeof(void*), cudaMemcpyHostToDevice);
            return;
        }
#endif

        int node_id = rank / device_count;
        int group_start = node_id * device_count;
        int group_end = std::min(group_start + device_count, num_ranks_);

        for (int dst = group_start; dst < group_end; ++dst) {
            if (active_ranks_mask[dst] == 0) continue;
            if (dst == rank) continue;

            int dst_device = dst % device_count;
            int can_access = 0;
            cudaDeviceCanAccessPeer(&can_access, device_id, dst_device);
            LOG(INFO) << "[EP P2P] rank " << rank << " (device " << device_id
                      << ") -> rank " << dst << " (device " << dst_device
                      << "): canAccessPeer=" << can_access;
            if (!can_access) continue;

#ifdef USE_MACA
            int src_physical = physicalDeviceFromVisibleList(device_id);
            int dst_physical = physicalDeviceFromVisibleList(dst_device);
            if (!macaP2pPairAllowed(src_physical, dst_physical)) {
                LOG(INFO) << "[EP P2P] rank " << rank << " physical GPU"
                          << src_physical << " -> rank " << dst
                          << " physical GPU" << dst_physical
                          << " disabled for MACA EP fast path; set "
                             "MOONCAKE_EP_MACA_ALLOW_NODE_P2P=1 or "
                             "MOONCAKE_EP_MACA_P2P_PAIRS to override";
                continue;
            }
#endif

            cudaError_t err = cudaDeviceEnablePeerAccess(dst_device, 0);
            if (err != cudaSuccess &&
                err != cudaErrorPeerAccessAlreadyEnabled) {
                LOG(WARNING) << "[EP P2P] rank " << rank
                             << " failed to enable peer access to device "
                             << dst_device << ": " << cudaGetErrorString(err);
                continue;
            }
            if (err == cudaErrorPeerAccessAlreadyEnabled) cudaGetLastError();

            if (dst >= static_cast<int>(remote_handles.size())) continue;
            const auto& h = remote_handles[dst];
            if (h.empty()) continue;

            constexpr size_t kHandleBytes = sizeof(cudaIpcMemHandle_t);
            constexpr size_t kNumInt32s =
                (kHandleBytes + sizeof(int32_t) - 1) / sizeof(int32_t);
#ifdef USE_MACA
            std::string ipc_mode = macaIpcMode();
            if (ipc_mode == "cross-v2" || ipc_mode == "cross_v2") {
                constexpr size_t kCrossHandleBytes =
                    sizeof(mcIpcCrossMemHandle_t);
                constexpr size_t kCrossNumInt32s =
                    (kCrossHandleBytes + sizeof(int32_t) - 1) / sizeof(int32_t);
                if (h.size() < kCrossNumInt32s) continue;
                mcIpcCrossMemHandle_t handle;
                memcpy(&handle, h.data(), kCrossHandleBytes);
                void* peer_ptr = nullptr;
                err = mcIpcOpenMemHandleCross_v2(
                    &peer_ptr, &handle, cudaIpcMemLazyEnablePeerAccess);
                if (err != cudaSuccess) {
                    LOG(WARNING)
                        << "[EP P2P] rank " << rank
                        << " failed to open cross_v2 IPC handle for rank "
                        << dst << ": " << cudaGetErrorString(err);
                    continue;
                }
                LOG(INFO) << "[EP P2P] rank " << rank
                          << " opened cross_v2 IPC handle for rank " << dst
                          << ": peer_ptr=" << peer_ptr;
                available[dst] = 1;
                peer_ptrs_host_[dst] = peer_ptr;
                continue;
            }
#endif
            if (h.size() < kNumInt32s) continue;

            cudaIpcMemHandle_t handle;
            memcpy(&handle, h.data(), kHandleBytes);

            void* peer_ptr = nullptr;
#ifdef USE_MACA
            err = ipc_mode == "cross"
                      ? mcIpcOpenMemHandleCross(&peer_ptr, handle,
                                                cudaIpcMemLazyEnablePeerAccess)
                      : cudaIpcOpenMemHandle(&peer_ptr, handle,
                                             cudaIpcMemLazyEnablePeerAccess);
#else
            err = cudaIpcOpenMemHandle(&peer_ptr, handle,
                                       cudaIpcMemLazyEnablePeerAccess);
#endif
            if (err != cudaSuccess) {
                LOG(WARNING) << "[EP P2P] rank " << rank
                             << " failed to open IPC handle for rank " << dst
                             << ": " << cudaGetErrorString(err);
                continue;
            }
            LOG(INFO) << "[EP P2P] rank " << rank
                      << " opened IPC handle for rank " << dst
                      << ": peer_ptr=" << peer_ptr;
            available[dst] = 1;
            peer_ptrs_host_[dst] = peer_ptr;
        }

        // Determine if all active ranks have P2P
        all_peers_accessible_ = true;
        for (int i = 0; i < num_ranks_; ++i) {
            if (active_ranks_mask[i] == 0) continue;
            if (!available[i] || !peer_ptrs_host_[i]) {
                all_peers_accessible_ = false;
                break;
            }
        }
        // Multi-node: P2P only within a node
        if (all_peers_accessible_ && num_ranks_ > 1) {
            int first_node = 0 / device_count;
            int last_node = (num_ranks_ - 1) / device_count;
            if (first_node != last_node) all_peers_accessible_ = false;
        }

        cudaMemcpy(available_table_, available.data(),
                   num_ranks_ * sizeof(int32_t), cudaMemcpyHostToDevice);
        cudaMemcpy(peer_ptrs_dev_, peer_ptrs_host_, num_ranks_ * sizeof(void*),
                   cudaMemcpyHostToDevice);
    }

    int32_t* availableTablePtr() override { return available_table_; }
    void** peerPtrsTablePtr() override { return peer_ptrs_dev_; }
    bool allPeersAccessible() const override { return all_peers_accessible_; }

    bool verifyPeerAccess() override {
        if (!all_peers_accessible_) return false;

        int device_id = 0;
        cudaGetDevice(&device_id);

        bool all_ok = true;
        for (int i = 0; i < num_ranks_; ++i) {
            if (i == device_id) continue;
            if (!peer_ptrs_host_[i] || peer_ptrs_host_[i] == local_ptr_)
                continue;

            // Test: write a pattern to the peer buffer via cudaMemcpy, then
            // read it back.  This verifies the IPC mapping is writable.
            constexpr int kTestBytes = 256;
            std::vector<uint8_t> pattern(kTestBytes);
            for (int j = 0; j < kTestBytes; ++j)
                pattern[j] = (uint8_t)(j ^ 0xA5);

            cudaError_t err = cudaMemcpy(peer_ptrs_host_[i], pattern.data(),
                                         kTestBytes, cudaMemcpyHostToDevice);
            if (err != cudaSuccess) {
                LOG(WARNING) << "[EP P2P] verifyPeerAccess: rank " << device_id
                             << " cannot write to peer " << i
                             << " mapped buffer: " << cudaGetErrorString(err);
                all_ok = false;
                continue;
            }

            // Read back and verify
            std::vector<uint8_t> readback(kTestBytes, 0);
            err = cudaMemcpy(readback.data(), peer_ptrs_host_[i], kTestBytes,
                             cudaMemcpyDeviceToHost);
            if (err != cudaSuccess) {
                LOG(WARNING) << "[EP P2P] verifyPeerAccess: rank " << device_id
                             << " cannot read back from peer " << i
                             << " mapped buffer: " << cudaGetErrorString(err);
                all_ok = false;
                continue;
            }

            bool match =
                (memcmp(readback.data(), pattern.data(), kTestBytes) == 0);
            if (!match) {
                LOG(WARNING) << "[EP P2P] verifyPeerAccess: rank " << device_id
                             << " readback mismatch from peer " << i;
                all_ok = false;
            } else {
                LOG(INFO) << "[EP P2P] verifyPeerAccess: rank " << device_id
                          << " peer " << i << " OK (memcpy write/read)";
            }
        }

        if (!all_ok) {
            all_peers_accessible_ = false;
            // Update device table to reflect failure
            std::vector<int32_t> avail_h(num_ranks_, 0);
            cudaMemcpy(avail_h.data(), available_table_,
                       num_ranks_ * sizeof(int32_t), cudaMemcpyDeviceToHost);
            for (int i = 0; i < num_ranks_; ++i) {
                if (i == device_id) continue;
                if (!peer_ptrs_host_[i] || peer_ptrs_host_[i] == local_ptr_)
                    continue;
                // Leave self as available; only clear failed peers
            }
            // Re-upload with all peers marked unavailable except self
            std::vector<int32_t> cleared(num_ranks_, 0);
            cleared[device_id] = 1;
            cudaMemcpy(available_table_, cleared.data(),
                       num_ranks_ * sizeof(int32_t), cudaMemcpyHostToDevice);
        }

        return all_ok;
    }

   private:
#if defined(USE_CUDA)
    struct FabricPeerMapping {
        void* ptr = nullptr;
        size_t size = 0;
        CUmemGenericAllocationHandle handle{};
    };

    void cleanupFabricPeerMappings() {
        if (!use_fabric_mem_) return;
        for (int i = 0; i < static_cast<int>(fabric_peer_mappings_.size());
             ++i) {
            auto& mapping = fabric_peer_mappings_[i];
            if (mapping.ptr == nullptr) continue;
            cuMemUnmap(reinterpret_cast<CUdeviceptr>(mapping.ptr),
                       mapping.size);
            cuMemAddressFree(reinterpret_cast<CUdeviceptr>(mapping.ptr),
                             mapping.size);
            cuMemRelease(mapping.handle);
            mapping = {};
            if (peer_ptrs_host_ && peer_ptrs_host_[i] != local_ptr_) {
                peer_ptrs_host_[i] = nullptr;
            }
        }
    }

    void cleanupFabricAllocations() {
        for (const auto& entry : fabric_allocations_) {
            const auto& allocation = entry.second;
            cuMemUnmap(allocation.ptr, allocation.size);
            cuMemAddressFree(allocation.ptr, allocation.size);
            cuMemRelease(allocation.handle);
        }
        fabric_allocations_.clear();
    }
#endif

    int num_ranks_;
    bool use_fabric_mem_ = false;
    void* local_ptr_ = nullptr;
    int32_t* available_table_ = nullptr;
    void** peer_ptrs_host_ = nullptr;
    void** peer_ptrs_dev_ = nullptr;
    bool all_peers_accessible_ = false;
#if defined(USE_CUDA)
    std::vector<FabricPeerMapping> fabric_peer_mappings_;
    std::unordered_map<void*, FabricAllocation> fabric_allocations_;
#endif
};

std::unique_ptr<P2pTransport> createP2pDeviceTransport(int num_ranks) {
    return std::make_unique<P2pDeviceTransportImpl>(num_ranks);
}

}  // namespace device
}  // namespace mooncake

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

#ifndef TENT_TRANSPORT_P2P_P2P_DEVICE_TRANSPORT_BASE_H_
#define TENT_TRANSPORT_P2P_P2P_DEVICE_TRANSPORT_BASE_H_

#include <cstring>
#include <vector>

#include <glog/logging.h>

#include "tent/runtime/device_transport.h"
#include "tent/transport/p2p/gpu_api_traits.h"

// Fabric memory (MNNVL) uses CUDA Driver API
#if defined(MOONCAKE_EP_USE_TENT) && !defined(MOONCAKE_EP_USE_MUSA)
#include <cuda.h>
#endif

namespace mooncake {
namespace tent {

/// Template base for P2P (NVLink/MTLink) device transports.
/// GpuApiTraits provides the GPU runtime API abstraction (CUDA or MUSA).
/// kSupportsFabric enables the fabric memory (MNNVL) path if true.
template <typename GpuApi, bool kSupportsFabric = false>
class P2pDeviceTransportBase : public DeviceTransport {
   public:
    // -------------------------------------------------------------------
    // DeviceTransport — capabilities
    // -------------------------------------------------------------------
    const DeviceCommCapabilities deviceCapabilities() const override {
        DeviceCommCapabilities caps;
        caps.nvlink_p2p = true;
        caps.signal = true;
        return caps;
    }

    // -------------------------------------------------------------------
    // DeviceTransport — GPU buffer allocation
    // -------------------------------------------------------------------
    Status allocateBuffer(void** ptr, size_t size,
                          bool allow_fabric) override {
        // Fabric memory path (MNNVL): only for CUDA with kSupportsFabric
        if constexpr (kSupportsFabric) {
            if (allow_fabric && tryFabricAlloc(ptr, size)) {
                use_fabric_mem_ = true;
                return Status::OK();
            }
        }
        // Fallback: plain GPU malloc
        use_fabric_mem_ = false;
        auto err = GpuApi::malloc(ptr, size);
        if (GpuApi::isError(err)) {
            return Status::InternalError(
                std::string(GpuApi::kTransportName) +
                " malloc in allocateBuffer failed: " +
                GpuApi::errorString(err));
        }
        return Status::OK();
    }

    Status freeBuffer(void* ptr) override {
        if (use_fabric_mem_) {
            freeFabric(ptr);
            use_fabric_mem_ = false;
            return Status::OK();
        }
        auto err = GpuApi::free(ptr);
        if (GpuApi::isError(err)) {
            return Status::InternalError(
                std::string(GpuApi::kTransportName) +
                " free in freeBuffer failed: " + GpuApi::errorString(err));
        }
        return Status::OK();
    }

    bool usesFabricMemory() const { return use_fabric_mem_; }

    // -------------------------------------------------------------------
    // DeviceTransport — P2P peer setup
    // -------------------------------------------------------------------
    Status allocatePeerAccessTables(int rank, int num_ranks) override {
        if (rank < 0 || num_ranks <= 0 || rank >= num_ranks) {
            return Status::InvalidArgument("Invalid rank/num_ranks");
        }
        release();
        rank_ = rank;
        num_ranks_ = num_ranks;

        CHECK_STATUS(GpuApi::checkStatus(
            GpuApi::malloc(reinterpret_cast<void**>(&available_),
                           num_ranks * sizeof(int32_t)),
            (std::string(GpuApi::kTransportName) + " malloc available failed")
               .c_str()));
        CHECK_STATUS(GpuApi::checkStatus(
            GpuApi::memset(available_, 0, num_ranks * sizeof(int32_t)),
            (std::string(GpuApi::kTransportName) + " memset available failed")
               .c_str()));

        CHECK_STATUS(GpuApi::checkStatus(
            GpuApi::mallocHost(reinterpret_cast<void**>(&host_peer_ptrs_),
                               num_ranks * sizeof(void*)),
            (std::string(GpuApi::kTransportName) +
             " mallocHost peer_ptrs failed")
               .c_str()));

        CHECK_STATUS(GpuApi::checkStatus(
            GpuApi::malloc(reinterpret_cast<void**>(&peer_ptrs_),
                           num_ranks * sizeof(void*)),
            (std::string(GpuApi::kTransportName) + " malloc peer_ptrs failed")
               .c_str()));
        CHECK_STATUS(GpuApi::checkStatus(
            GpuApi::memset(peer_ptrs_, 0, num_ranks * sizeof(void*)),
            (std::string(GpuApi::kTransportName) + " memset peer_ptrs failed")
               .c_str()));

        return Status::OK();
    }

    Status exportIpcHandle(int device_id, void* local_buffer,
                           std::vector<int32_t>& handle_words) override {
        typename GpuApi::IpcMemHandleType handle;
        // MUSA requires setting the device before IPC export
        if constexpr (!std::is_same_v<GpuApi, CudaApiTraits>) {
            int current_device = 0;
            GpuApi::getDevice(&current_device);
            GpuApi::setDevice(device_id);
        }

        auto err = GpuApi::ipcGetMemHandle(&handle, local_buffer);
        if (GpuApi::isError(err)) {
            return Status::InternalError(
                std::string(GpuApi::kTransportName) +
                " ipcGetMemHandle failed: " + GpuApi::errorString(err));
        }

        constexpr size_t num_words =
            (GpuApi::kIpcHandleSize + sizeof(int32_t) - 1) / sizeof(int32_t);
        handle_words.resize(num_words);
        std::memcpy(handle_words.data(), &handle, GpuApi::kIpcHandleSize);
        return Status::OK();
    }

    Status configurePeers(
        int local_device_id, void* local_buffer,
        const std::vector<std::vector<int32_t>>& remote_handles,
        const std::vector<int>& active_ranks_mask) override {
        if (!available_ || !peer_ptrs_ || !host_peer_ptrs_) {
            return Status::InternalError(std::string(GpuApi::kTransportName) +
                                 " peer tables have not been allocated" LOC_MARK);
        }

        // MUSA requires explicit device setting
        if constexpr (!std::is_same_v<GpuApi, CudaApiTraits>) {
            CHECK_STATUS(GpuApi::checkStatus(
                GpuApi::setDevice(local_device_id),
                (std::string(GpuApi::kTransportName) +
                 " setDevice in configurePeers failed")
                    .c_str()));
        }

        int num_devices = 0;
        CHECK_STATUS(GpuApi::checkStatus(
            GpuApi::getDeviceCount(&num_devices),
            (std::string("No ") +
             (std::is_same_v<GpuApi, CudaApiTraits> ? "CUDA" : "MUSA") +
             " devices found")
                .c_str()));
        if (num_devices == 0) {
            return Status::InternalError("No GPU devices found");
        }

        // Fabric memory path (NVLink/MNNVL only)
        if constexpr (kSupportsFabric) {
            bool use_fabric_memory = true;
            for (int i = 0; i < num_ranks_; ++i) {
                if (active_ranks_mask[i] && !remote_handles[i].empty()) {
                    use_fabric_memory = false;
                    break;
                }
            }
            if (use_fabric_memory) {
                for (int i = 0; i < num_ranks_; ++i) {
                    if (active_ranks_mask[i]) {
                        host_peer_ptrs_[i] =
                            (i == rank_) ? local_buffer : nullptr;
                    }
                }
                all_peers_accessible_ = (num_ranks_ == 1);
                goto copy_to_device;
            }
        }

        // Self peer pointer
        host_peer_ptrs_[rank_] = local_buffer;

        // Open IPC handles from remote ranks
        for (int dst_rank = 0; dst_rank < num_ranks_; ++dst_rank) {
            if (!active_ranks_mask[dst_rank]) continue;
            if (dst_rank == rank_) continue;

            // Enable peer access
            int can_access = 0;
            GpuApi::deviceCanAccessPeer(&can_access, local_device_id,
                                        dst_rank);
            if (can_access) {
                auto peer_err =
                    GpuApi::deviceEnablePeerAccess(dst_rank);
                if (GpuApi::isError(peer_err)) {
                    if (peer_err != GpuApi::kErrorPeerAccessAlreadyEnabled) {
                        LOG(ERROR) << GpuApi::kTransportName << ": rank "
                                   << rank_
                                   << " failed to enable peer access to rank "
                                   << dst_rank << ": "
                                   << GpuApi::errorString(peer_err);
                    }
                    GpuApi::getLastError();  // clear sticky error
                }
            }

            // Validate IPC handle
            if (remote_handles[dst_rank].empty()) {
                LOG(ERROR) << GpuApi::kTransportName << ": rank " << rank_
                           << " missing IPC handle for rank " << dst_rank;
                continue;
            }
            constexpr size_t num_words =
                (GpuApi::kIpcHandleSize + sizeof(int32_t) - 1) /
                sizeof(int32_t);
            if (remote_handles[dst_rank].size() < num_words) {
                LOG(ERROR) << GpuApi::kTransportName << ": rank " << rank_
                           << " invalid IPC handle size for rank " << dst_rank;
                continue;
            }

            typename GpuApi::IpcMemHandleType remote_handle;
            std::memcpy(&remote_handle, remote_handles[dst_rank].data(),
                        GpuApi::kIpcHandleSize);

            void* peer_ptr = nullptr;
            auto ipc_err = GpuApi::ipcOpenMemHandle(&peer_ptr, remote_handle);
            if (GpuApi::isError(ipc_err)) {
                LOG(ERROR) << GpuApi::kTransportName << ": rank " << rank_
                           << " failed to open IPC handle for rank "
                           << dst_rank << ": "
                           << GpuApi::errorString(ipc_err);
                continue;
            }

            host_peer_ptrs_[dst_rank] = peer_ptr;
            opened_peer_ptrs_.push_back(peer_ptr);
        }

        all_peers_accessible_ = true;
        for (int i = 0; i < num_ranks_; ++i) {
            if (active_ranks_mask[i] && !host_peer_ptrs_[i]) {
                all_peers_accessible_ = false;
                break;
            }
        }

    copy_to_device:
        // Copy tables to device
        std::vector<int32_t> available(num_ranks_, 0);
        for (int i = 0; i < num_ranks_; ++i) {
            if (active_ranks_mask[i] && host_peer_ptrs_[i]) {
                available[i] = 1;
            }
        }

        CHECK_STATUS(GpuApi::checkStatus(
            GpuApi::memcpy(available_, available.data(),
                           num_ranks_ * sizeof(int32_t),
                           GpuApi::kMemcpyHostToDevice),
            (std::string(GpuApi::kTransportName) +
             " memcpy availability failed")
               .c_str()));
        CHECK_STATUS(GpuApi::checkStatus(
            GpuApi::memcpy(peer_ptrs_, host_peer_ptrs_,
                           num_ranks_ * sizeof(void*),
                           GpuApi::kMemcpyHostToDevice),
            (std::string(GpuApi::kTransportName) +
             " memcpy peer pointers failed")
               .c_str()));

        // Update the GPU-visible device context struct.
        p2p_device_ctx_.abi_version = kP2pDeviceContextAbiVersion;
        p2p_device_ctx_.rank = rank_;
        p2p_device_ctx_.num_ranks = num_ranks_;
        p2p_device_ctx_.available = available_;
        p2p_device_ctx_.peer_ptrs = peer_ptrs_;
        return Status::OK();
    }

    bool allPeersAccessible() const override { return all_peers_accessible_; }

    // -------------------------------------------------------------------
    // DeviceTransport — RDMA setup (not supported by P2P)
    // -------------------------------------------------------------------
    Status initializeRdmaDevice(const std::string& /*device_name*/,
                                uint8_t /*port_num*/) override {
        return Status::NotSupported(
            std::string(GpuApi::kTransportName) +
            " does not support RDMA");
    }

    Status registerMemory(void* /*ptr*/, size_t /*size*/,
                          uint32_t& /*lkey*/, uint32_t& /*rkey*/) override {
        return Status::NotSupported(
            std::string(GpuApi::kTransportName) +
            " does not support RDMA memory registration");
    }

    Status unregisterMemory(void* /*ptr*/) override {
        return Status::NotSupported(
            std::string(GpuApi::kTransportName) +
            " does not support RDMA memory registration");
    }

    Status allocateControlBuffer(size_t /*size*/) override {
        return Status::NotSupported(
            std::string(GpuApi::kTransportName) +
            " does not use RDMA control buffers");
    }

    Status releaseControlBuffer() override {
        return Status::NotSupported(
            std::string(GpuApi::kTransportName) +
            " does not use RDMA control buffers");
    }

    void* controlBuffer() const override { return nullptr; }

    Status createQueuePairs(int /*num_qps*/, int /*wqe*/, void* /*stream*/,
                            void* /*qp_devctxs*/) override {
        return Status::NotSupported(
            std::string(GpuApi::kTransportName) +
            " does not use RDMA queue pairs");
    }

    Status recreateQueuePairs(int /*num_qps*/, int /*wqe*/, void* /*stream*/,
                              void* /*qp_devctxs*/) override {
        return Status::NotSupported(
            std::string(GpuApi::kTransportName) +
            " does not use RDMA queue pairs");
    }

    Status destroyQueuePairs() override {
        return Status::NotSupported(
            std::string(GpuApi::kTransportName) +
            " does not use RDMA queue pairs");
    }

    Status connectRdmaPeers(const RdmaPeerConnectInfo& /*info*/) override {
        return Status::NotSupported(
            std::string(GpuApi::kTransportName) +
            " does not support RDMA peer connections");
    }

    // -------------------------------------------------------------------
    // DeviceTransport — metadata accessors
    // -------------------------------------------------------------------
    IbGdaLocalMetadata localMetadata() const override { return {}; }

    bool isRoce() const override { return false; }

    int gidIndex() const override { return -1; }

    bool ibgdaDisabled() const override { return true; }

    // -------------------------------------------------------------------
    // DeviceTransport — GPU-kernel-visible context
    // -------------------------------------------------------------------
    const void* deviceContextPtr() const override {
        return &p2p_device_ctx_;
    }

    size_t deviceContextSize() const override {
        return sizeof(P2PDeviceContext);
    }

    DeviceContextAbi deviceContextAbi() const override {
        return DeviceContextAbi::kP2P;
    }

    // -------------------------------------------------------------------
    // DeviceTransport — GPU-kernel-visible tables (P2P)
    // -------------------------------------------------------------------
    int32_t* availableTablePtr() const override { return available_; }

    void** peerPtrsTablePtr() const override { return peer_ptrs_; }

    // -------------------------------------------------------------------
    // DeviceTransport — utility
    // -------------------------------------------------------------------
    void** hostPeerPtrs() const override { return host_peer_ptrs_; }

    void* getRemotePtr(void* local_ptr, int dst_rank) override {
        if (!host_peer_ptrs_ || dst_rank < 0 ||
            dst_rank >= num_ranks_) {
            return nullptr;
        }
        if (!host_peer_ptrs_[dst_rank]) return nullptr;
        auto offset = reinterpret_cast<uintptr_t>(local_ptr) -
                      reinterpret_cast<uintptr_t>(host_peer_ptrs_[rank_]);
        return reinterpret_cast<void*>(
            reinterpret_cast<uintptr_t>(host_peer_ptrs_[dst_rank]) + offset);
    }

   protected:
    // --- Fabric memory helpers (only compiled for CUDA + kSupportsFabric) ---

    /// Try to allocate using CUDA VMM / fabric memory. Returns true on success.
    bool tryFabricAlloc(void** ptr, size_t size) {
#if !defined(MOONCAKE_EP_USE_MUSA)
        if (!nvLinkSupportsFabricMemory()) return false;

        int device_id = 0;
        GpuApi::getDevice(&device_id);

        CUdevice cu_dev;
        CUresult res = cuDeviceGet(&cu_dev, device_id);
        if (res != CUDA_SUCCESS) return false;

        CUmemAllocationProp prop = {};
        prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
        prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        prop.location.id = cu_dev;
        prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;

        int rdma_flag = 0;
        cuDeviceGetAttribute(
            &rdma_flag,
            CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
            cu_dev);
        if (rdma_flag) prop.allocFlags.gpuDirectRDMACapable = 1;

        size_t granularity = 0;
        res = cuMemGetAllocationGranularity(&granularity, &prop,
                                            CU_MEM_ALLOC_GRANULARITY_MINIMUM);
        if (res != CUDA_SUCCESS) return false;

        fabric_alloc_size_ =
            (size + granularity - 1) & ~(granularity - 1);
        if (fabric_alloc_size_ == 0) fabric_alloc_size_ = granularity;

        res = cuMemCreate(&fabric_mem_handle_, fabric_alloc_size_, &prop, 0);
        if (res != CUDA_SUCCESS) return false;

        CUdeviceptr dptr = 0;
        res = cuMemAddressReserve(&dptr, fabric_alloc_size_, granularity, 0, 0);
        if (res != CUDA_SUCCESS) {
            cuMemRelease(fabric_mem_handle_);
            return false;
        }

        res = cuMemMap(dptr, fabric_alloc_size_, 0, fabric_mem_handle_, 0);
        if (res != CUDA_SUCCESS) {
            cuMemAddressFree(dptr, fabric_alloc_size_);
            cuMemRelease(fabric_mem_handle_);
            return false;
        }

        int device_count = 0;
        cudaGetDeviceCount(&device_count);
        std::vector<CUmemAccessDesc> access(device_count);
        for (int i = 0; i < device_count; ++i) {
            access[i].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
            access[i].location.id = i;
            access[i].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        }
        res = cuMemSetAccess(dptr, fabric_alloc_size_, access.data(),
                             device_count);
        if (res != CUDA_SUCCESS) {
            cuMemUnmap(dptr, fabric_alloc_size_);
            cuMemAddressFree(dptr, fabric_alloc_size_);
            cuMemRelease(fabric_mem_handle_);
            return false;
        }

        *ptr = reinterpret_cast<void*>(dptr);
        LOG(INFO) << GpuApi::kTransportName << ": Allocated "
                  << fabric_alloc_size_ << " bytes with fabric handle on GPU "
                  << device_id;
        return true;
#else
        (void)ptr;
        (void)size;
        return false;
#endif
    }

    /// Free a fabric-allocated buffer.
    void freeFabric(void* ptr) {
#if !defined(MOONCAKE_EP_USE_MUSA)
        CUdeviceptr dptr = reinterpret_cast<CUdeviceptr>(ptr);
        cuMemUnmap(dptr, fabric_alloc_size_);
        cuMemAddressFree(dptr, fabric_alloc_size_);
        cuMemRelease(fabric_mem_handle_);
        fabric_alloc_size_ = 0;
#endif
    }

    void release() {
        for (auto* ptr : opened_peer_ptrs_) {
            GpuApi::ipcCloseMemHandle(ptr);
        }
        opened_peer_ptrs_.clear();
        if (peer_ptrs_) GpuApi::free(peer_ptrs_);
        peer_ptrs_ = nullptr;
        if (available_) GpuApi::free(available_);
        available_ = nullptr;
        if (host_peer_ptrs_) GpuApi::freeHost(host_peer_ptrs_);
        host_peer_ptrs_ = nullptr;
        all_peers_accessible_ = false;
        rank_ = 0;
        num_ranks_ = 0;
    }

    int rank_ = 0;
    int num_ranks_ = 0;
    int32_t* available_ = nullptr;
    void** peer_ptrs_ = nullptr;
    void** host_peer_ptrs_ = nullptr;
    std::vector<void*> opened_peer_ptrs_;
    bool all_peers_accessible_ = false;
    P2PDeviceContext p2p_device_ctx_{};

    // Fabric memory state (only used when kSupportsFabric && CUDA)
    bool use_fabric_mem_ = false;
#if !defined(MOONCAKE_EP_USE_MUSA)
    CUmemGenericAllocationHandle fabric_mem_handle_{};
#endif
    size_t fabric_alloc_size_ = 0;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_P2P_P2P_DEVICE_TRANSPORT_BASE_H_

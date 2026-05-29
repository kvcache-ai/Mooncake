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

#include "tent/device/nvlink.h"

#include <cuda.h>
#include <cuda_runtime.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <string>

namespace mooncake {
namespace tent {

namespace {

Status cudaStatus(cudaError_t err, const char* message) {
    if (err == cudaSuccess) return Status::OK();
    return Status::InternalError(std::string(message) + ": " +
                                 cudaGetErrorString(err));
}

class NvLinkDeviceTransportImpl final : public NvLinkDeviceTransport {
   public:
    ~NvLinkDeviceTransportImpl() override { release(); }

    // -------------------------------------------------------------------
    // DeviceTransport — capabilities
    // -------------------------------------------------------------------
    const DeviceCommCapabilities deviceCapabilities() const override {
        DeviceCommCapabilities caps;
        caps.nvlink_p2p = all_peers_accessible_;
        caps.signal = true;
        caps.atomic = true;
        caps.latency_tier_ns = 500;  // sub-microsecond P2P
        return caps;
    }

    // -------------------------------------------------------------------
    // DeviceTransport — GPU buffer allocation
    // -------------------------------------------------------------------
    Status allocateBuffer(void** ptr, size_t size,
                          bool /*allow_fabric*/) override {
        return cudaStatus(cudaMalloc(ptr, size),
                          "cudaMalloc in NvLink allocateBuffer failed");
    }

    Status freeBuffer(void* ptr) override {
        return cudaStatus(cudaFree(ptr), "cudaFree in NvLink freeBuffer failed");
    }

    // -------------------------------------------------------------------
    // DeviceTransport — P2P peer setup
    // -------------------------------------------------------------------
    Status allocatePeerAccessTables(int rank, int num_ranks) override {
        release();
        rank_ = rank;
        num_ranks_ = num_ranks;
        all_peers_accessible_ = false;

        CHECK_STATUS(cudaStatus(
            cudaMalloc(&available_, num_ranks * sizeof(int32_t)),
            "cudaMalloc nvlink availability table failed"));
        CHECK_STATUS(cudaStatus(
            cudaMemset(available_, 0, num_ranks * sizeof(int32_t)),
            "cudaMemset nvlink availability table failed"));
        CHECK_STATUS(cudaStatus(cudaMallocHost(&host_peer_ptrs_,
                                               num_ranks * sizeof(void*)),
                                "cudaMallocHost peer pointer table failed"));
        CHECK_STATUS(cudaStatus(cudaMalloc(&peer_ptrs_, num_ranks * sizeof(void*)),
                                "cudaMalloc peer pointer table failed"));
        for (int i = 0; i < num_ranks; ++i) host_peer_ptrs_[i] = nullptr;
        CHECK_STATUS(cudaStatus(cudaMemset(peer_ptrs_, 0, num_ranks * sizeof(void*)),
                                "cudaMemset peer pointer table failed"));
        return Status::OK();
    }

    Status exportIpcHandle(int /*device_id*/, void* local_buffer,
                           std::vector<int32_t>& handle_words) override {
        cudaIpcMemHandle_t cuda_handle;
        CHECK_STATUS(cudaStatus(cudaIpcGetMemHandle(&cuda_handle, local_buffer),
                                "cudaIpcGetMemHandle failed"));

        constexpr size_t handle_size = sizeof(cudaIpcMemHandle_t);
        const size_t num_words =
            (handle_size + sizeof(int32_t) - 1) / sizeof(int32_t);
        handle_words.assign(num_words, 0);
        std::memcpy(handle_words.data(), &cuda_handle, handle_size);
        return Status::OK();
    }

    Status configurePeers(
        int local_device_id, void* local_buffer,
        const std::vector<std::vector<int32_t>>& remote_handles,
        const std::vector<int>& active_ranks_mask) override {
        if (!available_ || !peer_ptrs_ || !host_peer_ptrs_) {
            return Status::InvalidArgument(
                "NVLink peer tables have not been allocated" LOC_MARK);
        }
        if (static_cast<int>(active_ranks_mask.size()) < num_ranks_) {
            return Status::InvalidArgument(
                "active_ranks_mask is smaller than num_ranks" LOC_MARK);
        }

        int device_count = 0;
        CHECK_STATUS(cudaStatus(cudaGetDeviceCount(&device_count),
                                "cudaGetDeviceCount failed"));
        if (device_count <= 0) {
            return Status::InternalError("No CUDA devices found" LOC_MARK);
        }

        std::vector<int32_t> available(num_ranks_, 0);
        available[rank_] = 1;

        // Check if fabric memory is in use (no IPC handles needed).
        bool use_fabric_memory = true;
        for (int i = 0; i < num_ranks_; ++i) {
            if (active_ranks_mask[i] == 0) continue;
            if (i != rank_ && i < static_cast<int>(remote_handles.size()) &&
                !remote_handles[i].empty()) {
                use_fabric_memory = false;
                break;
            }
        }

        if (use_fabric_memory) {
            for (int i = 0; i < num_ranks_; ++i) {
                if (active_ranks_mask[i] == 0) continue;
                host_peer_ptrs_[i] = (i == rank_) ? local_buffer : nullptr;
                available[i] = host_peer_ptrs_[i] ? 1 : 0;
            }
            all_peers_accessible_ = num_ranks_ == 1;
        } else {
            const int node_id = rank_ / device_count;
            const int group_start = node_id * device_count;
            const int group_end = std::min(group_start + device_count, num_ranks_);

            for (int dst_rank = group_start; dst_rank < group_end; ++dst_rank) {
                if (active_ranks_mask[dst_rank] == 0) continue;
                if (dst_rank == rank_) {
                    host_peer_ptrs_[dst_rank] = local_buffer;
                    continue;
                }

                const int dst_device = dst_rank % device_count;
                int can_access_peer = 0;
                auto err = cudaDeviceCanAccessPeer(&can_access_peer,
                                                   local_device_id, dst_device);
                if (err != cudaSuccess || !can_access_peer) continue;

                auto peer_err = cudaDeviceEnablePeerAccess(dst_device, 0);
                if (peer_err == cudaErrorPeerAccessAlreadyEnabled) {
                    cudaGetLastError();
                } else if (peer_err != cudaSuccess) {
                    LOG(WARNING) << "NVLink: rank " << rank_
                                 << " failed to enable peer access to rank "
                                 << dst_rank << ": "
                                 << cudaGetErrorString(peer_err);
                    continue;
                }

                if (dst_rank >= static_cast<int>(remote_handles.size()) ||
                    remote_handles[dst_rank].empty()) {
                    LOG(WARNING) << "NVLink: rank " << rank_
                                 << " missing IPC handle for rank " << dst_rank;
                    continue;
                }

                constexpr size_t handle_size = sizeof(cudaIpcMemHandle_t);
                const size_t num_words =
                    (handle_size + sizeof(int32_t) - 1) / sizeof(int32_t);
                const auto& words = remote_handles[dst_rank];
                if (words.size() < num_words) {
                    LOG(WARNING) << "NVLink: rank " << rank_
                                 << " invalid IPC handle size for rank "
                                 << dst_rank;
                    continue;
                }

                cudaIpcMemHandle_t remote_handle;
                std::memcpy(&remote_handle, words.data(), handle_size);
                void* peer_ptr = nullptr;
                auto ipc_err = cudaIpcOpenMemHandle(
                    &peer_ptr, remote_handle, cudaIpcMemLazyEnablePeerAccess);
                if (ipc_err != cudaSuccess) {
                    LOG(WARNING) << "NVLink: rank " << rank_
                                 << " failed to open IPC handle for rank "
                                 << dst_rank << ": "
                                 << cudaGetErrorString(ipc_err);
                    continue;
                }
                available[dst_rank] = 1;
                host_peer_ptrs_[dst_rank] = peer_ptr;
                opened_peer_ptrs_.push_back(peer_ptr);
            }

            all_peers_accessible_ = true;
            for (int i = 0; i < num_ranks_; ++i) {
                if (active_ranks_mask[i] == 0) continue;
                if (available[i] == 0 || host_peer_ptrs_[i] == nullptr) {
                    all_peers_accessible_ = false;
                    break;
                }
            }
            if (all_peers_accessible_ && num_ranks_ > 1) {
                const int first_node_id = 0 / device_count;
                const int last_node_id = (num_ranks_ - 1) / device_count;
                if (first_node_id != last_node_id) all_peers_accessible_ = false;
            }
        }

        CHECK_STATUS(cudaStatus(cudaMemcpy(available_, available.data(),
                                           num_ranks_ * sizeof(int32_t),
                                           cudaMemcpyHostToDevice),
                                "cudaMemcpy nvlink availability failed"));
        CHECK_STATUS(cudaStatus(cudaMemcpy(peer_ptrs_, host_peer_ptrs_,
                                           num_ranks_ * sizeof(void*),
                                           cudaMemcpyHostToDevice),
                                "cudaMemcpy nvlink peer pointers failed"));

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
    // DeviceTransport — RDMA setup (not supported by NVLink)
    // -------------------------------------------------------------------
    Status initializeRdmaDevice(const std::string& /*device_name*/,
                                uint8_t /*port_num*/) override {
        return Status::NotSupported("NVLink does not support RDMA");
    }

    Status registerMemory(void* /*ptr*/, size_t /*size*/,
                          uint32_t& /*lkey*/, uint32_t& /*rkey*/) override {
        return Status::NotSupported("NVLink does not support RDMA memory registration");
    }

    Status unregisterMemory(void* /*ptr*/) override {
        return Status::NotSupported("NVLink does not support RDMA memory registration");
    }

    Status allocateControlBuffer(size_t /*size*/) override {
        return Status::NotSupported("NVLink does not use RDMA control buffers");
    }

    Status releaseControlBuffer() override {
        return Status::NotSupported("NVLink does not use RDMA control buffers");
    }

    void* controlBuffer() const override { return nullptr; }

    Status createQueuePairs(int /*num_qps*/, int /*wqe*/, void* /*stream*/,
                            void* /*qp_devctxs*/) override {
        return Status::NotSupported("NVLink does not use RDMA queue pairs");
    }

    Status recreateQueuePairs(int /*num_qps*/, int /*wqe*/, void* /*stream*/,
                              void* /*qp_devctxs*/) override {
        return Status::NotSupported("NVLink does not use RDMA queue pairs");
    }

    Status destroyQueuePairs() override {
        return Status::NotSupported("NVLink does not use RDMA queue pairs");
    }

    Status connectRdmaPeers(const RdmaPeerConnectInfo& /*info*/) override {
        return Status::NotSupported("NVLink does not support RDMA peer connections");
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

   private:
    void release() {
        for (void* ptr : opened_peer_ptrs_) cudaIpcCloseMemHandle(ptr);
        opened_peer_ptrs_.clear();
        if (peer_ptrs_) cudaFree(peer_ptrs_);
        peer_ptrs_ = nullptr;
        if (available_) cudaFree(available_);
        available_ = nullptr;
        if (host_peer_ptrs_) cudaFreeHost(host_peer_ptrs_);
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
};

}  // namespace

std::unique_ptr<NvLinkDeviceTransport> createNvLinkDeviceTransport() {
    return std::make_unique<NvLinkDeviceTransportImpl>();
}

bool nvLinkSupportsFabricMemory() {
    const char* nvlink_ipc = getenv("MC_USE_NVLINK_IPC");
    const bool fabric_enabled = nvlink_ipc && std::strcmp(nvlink_ipc, "0") == 0;
    if (!fabric_enabled) return false;

    int num_devices = 0;
    auto err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess || num_devices == 0) return false;

    for (int dev = 0; dev < num_devices; ++dev) {
        int supported = 0;
        auto res = cuDeviceGetAttribute(
            &supported, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED, dev);
        if (res != CUDA_SUCCESS || !supported) return false;
    }
    return true;
}

}  // namespace tent
}  // namespace mooncake

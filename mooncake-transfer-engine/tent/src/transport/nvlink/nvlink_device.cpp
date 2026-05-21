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

    Status exportIpcHandle(void* local_buffer,
                           NvLinkIpcHandle& handle) override {
        cudaIpcMemHandle_t cuda_handle;
        CHECK_STATUS(cudaStatus(cudaIpcGetMemHandle(&cuda_handle, local_buffer),
                                "cudaIpcGetMemHandle failed"));

        constexpr size_t handle_size = sizeof(cudaIpcMemHandle_t);
        const size_t num_words =
            (handle_size + sizeof(int32_t) - 1) / sizeof(int32_t);
        handle.words.assign(num_words, 0);
        std::memcpy(handle.words.data(), &cuda_handle, handle_size);
        return Status::OK();
    }

    Status configurePeers(int local_device_id, void* local_buffer,
                          const std::vector<NvLinkIpcHandle>& remote_handles,
                          const std::vector<int>& active_ranks_mask,
                          bool use_fabric_memory) override {
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

        if (use_fabric_memory) {
            for (int i = 0; i < num_ranks_; ++i) {
                if (active_ranks_mask[i] == 0) continue;
                // TENT exposes the same ABI for fabric memory, but EP kernels
                // still need a valid per-rank base pointer.  Until the caller
                // exchanges fabric virtual addresses explicitly, only the local
                // rank pointer is usable.
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
                    remote_handles[dst_rank].words.empty()) {
                    LOG(WARNING) << "NVLink: rank " << rank_
                                 << " missing IPC handle for rank " << dst_rank;
                    continue;
                }

                constexpr size_t handle_size = sizeof(cudaIpcMemHandle_t);
                const size_t num_words =
                    (handle_size + sizeof(int32_t) - 1) / sizeof(int32_t);
                const auto& words = remote_handles[dst_rank].words;
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
        return Status::OK();
    }

    NvLinkDeviceContext deviceContext() const override {
        return NvLinkDeviceContext{kNvLinkDeviceContextAbiVersion, rank_,
                                   num_ranks_, available_, peer_ptrs_};
    }

    bool allPeersAccessible() const override { return all_peers_accessible_; }

    void** hostPeerPtrs() const override { return host_peer_ptrs_; }

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

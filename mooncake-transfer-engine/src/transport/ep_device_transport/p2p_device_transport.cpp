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

#include "transport/ep_device_transport/device_transport.h"

#include <glog/logging.h>
#include <algorithm>
#include <cstring>

#include "cuda_alike.h"

namespace mooncake {
namespace ep {

class P2pDeviceTransportImpl : public P2pTransport {
   public:
    explicit P2pDeviceTransportImpl(int num_ranks)
        : num_ranks_(num_ranks) {
        cudaMalloc(&available_table_, num_ranks_ * sizeof(int32_t));
        cudaMemset(available_table_, 0, num_ranks_ * sizeof(int32_t));
        cudaMallocHost(&peer_ptrs_host_, num_ranks_ * sizeof(void*));
        cudaMalloc(&peer_ptrs_dev_, num_ranks_ * sizeof(void*));
        for (int i = 0; i < num_ranks_; ++i) peer_ptrs_host_[i] = nullptr;
        cudaMemset(peer_ptrs_dev_, 0, num_ranks_ * sizeof(void*));
    }

    ~P2pDeviceTransportImpl() override {
        if (available_table_) cudaFree(available_table_);
        if (peer_ptrs_dev_) cudaFree(peer_ptrs_dev_);
        if (peer_ptrs_host_) {
            for (int i = 0; i < num_ranks_; ++i) {
                if (peer_ptrs_host_[i] && peer_ptrs_host_[i] != local_ptr_) {
                    cudaIpcCloseMemHandle(peer_ptrs_host_[i]);
                }
            }
            cudaFreeHost(peer_ptrs_host_);
        }
    }

    void* allocateBuffer(size_t bytes) override {
        void* ptr = nullptr;
        cudaError_t err = cudaMalloc(&ptr, bytes);
        if (err != cudaSuccess) {
            LOG(ERROR) << "[EP P2P] cudaMalloc(" << bytes
                       << ") failed: " << cudaGetErrorString(err);
            return nullptr;
        }
        return ptr;
    }

    void freeBuffer(void* ptr) override { cudaFree(ptr); }

    std::vector<int32_t> exportIpcHandle(void* ptr) override {
        cudaIpcMemHandle_t handle;
        cudaError_t err = cudaIpcGetMemHandle(&handle, ptr);
        if (err != cudaSuccess) {
            LOG(ERROR) << "[EP P2P] cudaIpcGetMemHandle failed: "
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

        std::vector<int32_t> available(num_ranks_, 0);
        available[rank] = 1;
        peer_ptrs_host_[rank] = local_ptr;

        int node_id = rank / device_count;
        int group_start = node_id * device_count;
        int group_end = std::min(group_start + device_count, num_ranks_);

        for (int dst = group_start; dst < group_end; ++dst) {
            if (active_ranks_mask[dst] == 0) continue;
            if (dst == rank) continue;

            int dst_device = dst % device_count;
            int can_access = 0;
            cudaDeviceCanAccessPeer(&can_access, device_id, dst_device);
            if (!can_access) continue;

            cudaError_t err = cudaDeviceEnablePeerAccess(dst_device, 0);
            if (err != cudaSuccess &&
                err != cudaErrorPeerAccessAlreadyEnabled) {
                continue;
            }
            if (err == cudaErrorPeerAccessAlreadyEnabled) cudaGetLastError();

            if (dst >= static_cast<int>(remote_handles.size())) continue;
            const auto& h = remote_handles[dst];
            if (h.empty()) continue;

            constexpr size_t kHandleBytes = sizeof(cudaIpcMemHandle_t);
            constexpr size_t kNumInt32s =
                (kHandleBytes + sizeof(int32_t) - 1) / sizeof(int32_t);
            if (h.size() < kNumInt32s) continue;

            cudaIpcMemHandle_t handle;
            memcpy(&handle, h.data(), kHandleBytes);

            void* peer_ptr = nullptr;
            err = cudaIpcOpenMemHandle(&peer_ptr, handle,
                                       cudaIpcMemLazyEnablePeerAccess);
            if (err != cudaSuccess) {
                LOG(WARNING) << "[EP P2P] rank " << rank
                             << " failed to open IPC handle for rank " << dst
                             << ": " << cudaGetErrorString(err);
                continue;
            }
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
        cudaMemcpy(peer_ptrs_dev_, peer_ptrs_host_,
                   num_ranks_ * sizeof(void*), cudaMemcpyHostToDevice);
    }

    int32_t* availableTablePtr() override { return available_table_; }
    void** peerPtrsTablePtr() override { return peer_ptrs_dev_; }
    bool allPeersAccessible() const override { return all_peers_accessible_; }

   private:
    int num_ranks_;
    void* local_ptr_ = nullptr;
    int32_t* available_table_ = nullptr;
    void** peer_ptrs_host_ = nullptr;
    void** peer_ptrs_dev_ = nullptr;
    bool all_peers_accessible_ = false;
};

std::unique_ptr<P2pTransport> createP2pDeviceTransport(int num_ranks) {
    return std::make_unique<P2pDeviceTransportImpl>(num_ranks);
}

}  // namespace ep
}  // namespace mooncake

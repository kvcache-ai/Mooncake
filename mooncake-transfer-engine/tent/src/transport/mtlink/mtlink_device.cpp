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

// MTLink P2P transport for Moore Threads GPUs.
//
// Uses musaIpcGetMemHandle / musaIpcOpenMemHandle for cross-process P2P,
// following the same pattern as NVLink on CUDA.  After musaIpcOpenMemHandle
// maps the peer's buffer into the local process's address space, the
// returned pointer is directly usable by device kernels for P2P read/write.
//
// Peer access (musaDeviceEnablePeerAccess) is also enabled so that device
// kernels can directly access peer memory through the IPC-mapped pointers.

#include "tent/device/mtlink.h"

#include <musa_runtime.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <string>

namespace mooncake {
namespace tent {

namespace {

Status musaStatus(musaError_t err, const char* message) {
    if (err == musaSuccess) return Status::OK();
    return Status::InternalError(std::string(message) + ": " +
                                 musaGetErrorString(err));
}

// Number of int32_t words needed to encode a musaIpcMemHandle_t.
// On current MUSA platforms, musaIpcMemHandle_t is 64 bytes.
constexpr size_t kIpcHandleWords =
    sizeof(musaIpcMemHandle_t) / sizeof(int32_t);

class MtLinkDeviceTransportImpl final : public MtLinkDeviceTransport {
   public:
    ~MtLinkDeviceTransportImpl() override { release(); }

    Status allocatePeerAccessTables(int rank, int num_ranks) override {
        release();
        rank_ = rank;
        num_ranks_ = num_ranks;
        all_peers_accessible_ = false;

        CHECK_STATUS(musaStatus(
            musaMalloc(&available_, num_ranks * sizeof(int32_t)),
            "musaMalloc mtlink availability table failed"));
        CHECK_STATUS(musaStatus(
            musaMemset(available_, 0, num_ranks * sizeof(int32_t)),
            "musaMemset mtlink availability table failed"));
        CHECK_STATUS(musaStatus(
            musaMallocHost(&host_peer_ptrs_, num_ranks * sizeof(void*)),
            "musaMallocHost peer pointer table failed"));
        CHECK_STATUS(musaStatus(
            musaMalloc(&peer_ptrs_, num_ranks * sizeof(void*)),
            "musaMalloc peer pointer table failed"));
        for (int i = 0; i < num_ranks; ++i) host_peer_ptrs_[i] = nullptr;
        CHECK_STATUS(musaStatus(
            musaMemset(peer_ptrs_, 0, num_ranks * sizeof(void*)),
            "musaMemset peer pointer table failed"));
        return Status::OK();
    }

    // Export the local buffer's IPC handle using musaIpcGetMemHandle.
    Status exportIpcHandle(int device_id, void* local_buffer,
                           MtLinkIpcHandle& handle) override {
        // musaIpcGetMemHandle requires the device to be set to the one
        // where the buffer was allocated.
        int current_device = -1;
        musaGetDevice(&current_device);
        LOG(INFO) << "MTLink exportIpcHandle: device_id=" << device_id
                  << " current_device=" << current_device
                  << " local_buffer=" << local_buffer;
        CHECK_STATUS(musaStatus(musaSetDevice(device_id),
                                "musaSetDevice in exportIpcHandle failed"));
        musaIpcMemHandle_t ipc_handle;
        auto err = musaIpcGetMemHandle(&ipc_handle, local_buffer);
        if (err != musaSuccess) {
            LOG(ERROR) << "MTLink exportIpcHandle: musaIpcGetMemHandle failed: "
                       << musaGetErrorString(err)
                       << " (err=" << err << ")"
                       << " device_id=" << device_id
                       << " local_buffer=" << local_buffer;
        }
        CHECK_STATUS(musaStatus(err, "musaIpcGetMemHandle failed"));
        handle.words.assign(kIpcHandleWords, 0);
        std::memcpy(handle.words.data(), &ipc_handle,
                    sizeof(musaIpcMemHandle_t));
        return Status::OK();
    }

    Status configurePeers(
        int local_device_id, void* local_buffer,
        const std::vector<MtLinkIpcHandle>& remote_handles,
        const std::vector<int>& active_ranks_mask) override {
        if (!available_ || !peer_ptrs_ || !host_peer_ptrs_) {
            return Status::InvalidArgument(
                "MTLink peer tables have not been allocated" LOC_MARK);
        }
        if (static_cast<int>(active_ranks_mask.size()) < num_ranks_) {
            return Status::InvalidArgument(
                "active_ranks_mask is smaller than num_ranks" LOC_MARK);
        }

        // Set the correct device before any MUSA operations.
        CHECK_STATUS(musaStatus(musaSetDevice(local_device_id),
                                "musaSetDevice in configurePeers failed"));

        int device_count = 0;
        CHECK_STATUS(musaStatus(musaGetDeviceCount(&device_count),
                                "musaGetDeviceCount failed"));
        if (device_count <= 0) {
            return Status::InternalError("No MUSA devices found" LOC_MARK);
        }

        std::vector<int32_t> available(num_ranks_, 0);
        available[rank_] = 1;
        host_peer_ptrs_[rank_] = local_buffer;

        const int node_id = rank_ / device_count;
        const int group_start = node_id * device_count;
        const int group_end = std::min(group_start + device_count, num_ranks_);

        for (int dst_rank = group_start; dst_rank < group_end; ++dst_rank) {
            if (active_ranks_mask[dst_rank] == 0) continue;
            if (dst_rank == rank_) continue;  // already set above

            // Enable P2P access to the peer device
            const int dst_device = dst_rank % device_count;
            int can_access_peer = 0;
            auto err = musaDeviceCanAccessPeer(&can_access_peer,
                                               local_device_id, dst_device);
            if (err != musaSuccess || !can_access_peer) {
                LOG(WARNING) << "MTLink: rank " << rank_
                             << " cannot access peer device " << dst_device;
                continue;
            }

            auto peer_err = musaDeviceEnablePeerAccess(dst_device, 0);
            if (peer_err == musaErrorPeerAccessAlreadyEnabled) {
                musaGetLastError();
            } else if (peer_err != musaSuccess) {
                LOG(WARNING) << "MTLink: rank " << rank_
                             << " failed to enable peer access to device "
                             << dst_device << ": "
                             << musaGetErrorString(peer_err);
                continue;
            }

            // Decode the peer's IPC handle and open it
            if (dst_rank >= static_cast<int>(remote_handles.size()) ||
                remote_handles[dst_rank].words.size() < kIpcHandleWords) {
                LOG(WARNING) << "MTLink: rank " << rank_
                             << " missing or invalid IPC handle for rank "
                             << dst_rank;
                continue;
            }

            musaIpcMemHandle_t remote_handle;
            std::memcpy(&remote_handle, remote_handles[dst_rank].words.data(),
                        sizeof(musaIpcMemHandle_t));

            void* peer_ptr = nullptr;
            auto ipc_err = musaIpcOpenMemHandle(
                &peer_ptr, remote_handle, musaIpcMemLazyEnablePeerAccess);
            if (ipc_err != musaSuccess) {
                LOG(WARNING) << "MTLink: rank " << rank_
                             << " failed to open IPC handle for rank "
                             << dst_rank << ": "
                             << musaGetErrorString(ipc_err);
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

        CHECK_STATUS(musaStatus(
            musaMemcpy(available_, available.data(),
                       num_ranks_ * sizeof(int32_t), musaMemcpyHostToDevice),
            "musaMemcpy mtlink availability failed"));
        CHECK_STATUS(musaStatus(
            musaMemcpy(peer_ptrs_, host_peer_ptrs_,
                       num_ranks_ * sizeof(void*), musaMemcpyHostToDevice),
            "musaMemcpy mtlink peer pointers failed"));
        return Status::OK();
    }

    MtLinkDeviceContext deviceContext() const override {
        return MtLinkDeviceContext{kMtLinkDeviceContextAbiVersion, rank_,
                                  num_ranks_, available_, peer_ptrs_};
    }

    bool allPeersAccessible() const override { return all_peers_accessible_; }

    void** hostPeerPtrs() const override { return host_peer_ptrs_; }

   private:
    void release() {
        // Close IPC handles before freeing device arrays
        for (auto* ptr : opened_peer_ptrs_) {
            musaIpcCloseMemHandle(ptr);
        }
        opened_peer_ptrs_.clear();
        if (peer_ptrs_) musaFree(peer_ptrs_);
        peer_ptrs_ = nullptr;
        if (available_) musaFree(available_);
        available_ = nullptr;
        if (host_peer_ptrs_) musaFreeHost(host_peer_ptrs_);
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

std::unique_ptr<MtLinkDeviceTransport> createMtLinkDeviceTransport() {
    return std::make_unique<MtLinkDeviceTransportImpl>();
}

}  // namespace tent
}  // namespace mooncake

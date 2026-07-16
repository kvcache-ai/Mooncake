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
#include <cstdlib>
#include <cstring>
#include <string>

#include "cuda_alike.h"
#include "environ.h"

namespace mooncake {
namespace device {

#ifdef USE_MACA
namespace {

bool parseBoolEnv(const char* name) { return Environ::GetBool(name, false); }

std::string getLowerEnv(const char* name) {
    std::string s = Environ::GetString(name, "");
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

    std::string explicit_pairs = Environ::Get().GetEpMacaP2pPairs();
    if (!explicit_pairs.empty())
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
    explicit P2pDeviceTransportImpl(int num_ranks) : num_ranks_(num_ranks) {
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

    void freeBuffer(void* ptr) override { cudaFree(ptr); }

    std::vector<int32_t> exportIpcHandle(void* ptr) override {
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

}  // namespace device
}  // namespace mooncake

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

#ifndef TENT_RUNTIME_DEVICE_RESOURCES_H_
#define TENT_RUNTIME_DEVICE_RESOURCES_H_

#include <cstdint>
#include <vector>

namespace mooncake {
namespace tent {

inline constexpr int kIbGdaMaxQueuePairs = 256;

struct DeviceCommCapabilities {
    bool gpu_initiated_rdma = false;
    bool symmetric_memory = false;
    bool nvlink_p2p = false;
    bool signal = false;
    bool atomic = false;
    int latency_tier_ns = 0;
};

struct DeviceChannelResources {
    int channel_id = 0;
    int num_channels = 0;
    void* network_ctx = nullptr;
};

struct DevicePeerInfo {
    uint64_t raddr = 0;
    uint32_t rkey = 0;
    uint32_t qp_num = 0;
};

// Host-visible IBGDA metadata that callers exchange before connecting
// GPU-initiated IBGDA queue pairs.  Moved here from ibgda.h so the
// DeviceTransport interface can reference it without pulling in
// IBGDA-specific headers.
struct IbGdaLocalMetadata {
    int64_t raddr = 0;
    int32_t rkey = 0;
    bool is_roce = false;
    int64_t subnet_prefix = 0;
    int64_t interface_id = 0;
    std::vector<int32_t> qpns;
    std::vector<int32_t> lids;
};

inline constexpr uint32_t kIbGdaDeviceContextAbiVersion = 1;

// GPU-visible IBGDA resources consumed by EP kernels.  TENT owns this ABI so
// mooncake-ep can gradually stop depending on its legacy mlx5gda host layout.
// The pointer fields refer to device allocations unless otherwise documented by
// the transport implementation.
struct IbGdaDeviceContext {
    uint32_t abi_version = kIbGdaDeviceContextAbiVersion;
    int rank = 0;
    int num_ranks = 0;
    int num_qps = 0;
    void* raddrs = nullptr;
    void* rkeys = nullptr;
    void* qp_devctxs = nullptr;
};

inline constexpr uint32_t kNvLinkDeviceContextAbiVersion = 1;

// GPU-visible NVLink/P2P resources consumed by device kernels.  `available`
// is a device array of length `num_ranks`; `peer_ptrs` is a device array of
// peer base pointers indexed by rank.  Kernels can compute a peer address by
// adding the local-buffer-relative offset to `peer_ptrs[dst_rank]` when
// `available[dst_rank] != 0`.
struct NvLinkDeviceContext {
    uint32_t abi_version = kNvLinkDeviceContextAbiVersion;
    int rank = 0;
    int num_ranks = 0;
    int32_t* available = nullptr;
    void** peer_ptrs = nullptr;
};

inline constexpr uint32_t kMtLinkDeviceContextAbiVersion = 1;

// GPU-visible MTLink/P2P resources for Moore Threads GPUs.  Structurally
// identical to NvLinkDeviceContext — same IPC-based P2P model with peer base
// pointers and availability flags.  Separate type for future divergence.
struct MtLinkDeviceContext {
    uint32_t abi_version = kMtLinkDeviceContextAbiVersion;
    int rank = 0;
    int num_ranks = 0;
    int32_t* available = nullptr;
    void** peer_ptrs = nullptr;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_DEVICE_RESOURCES_H_

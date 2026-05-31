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

#pragma once

// EP DeviceTransport — platform-agnostic host-side abstraction for the two
// communication paths used by the EP kernel:
//
//   P2pTransport   — intra-node GPU-initiated P2P (NVLink on CUDA, MTLink on
//                    MUSA).  Manages IPC handle exchange and peer pointer table.
//   RdmaTransport  — inter-node GPU-initiated RDMA (IBGDA / mlx5gda).
//                    Manages QP lifecycle, MR, and device context table.
//
// EP code includes only this header and calls the abstract interface.
// Platform-specific implementations live in device/ and are
// selected at build time by the factory functions at the bottom of this file.
//
// The header intentionally avoids including cuda_alike.h so it can be included
// from pure C++ translation units.  Implementations include cuda_alike.h.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace mooncake {
namespace device {

// ---------------------------------------------------------------------------
// P2pTransport
//
// Manages intra-node GPU P2P IPC handles and the device-visible peer pointer
// table used by the EP kernel.
// ---------------------------------------------------------------------------
class P2pTransport {
   public:
    virtual ~P2pTransport() = default;

    // Allocate the GDR buffer that will be shared via IPC.
    // Returns a device pointer; size must be > 0.
    virtual void* allocateBuffer(size_t bytes) = 0;

    // Free a buffer previously returned by allocateBuffer.
    virtual void freeBuffer(void* ptr) = 0;

    // Export an IPC handle for the buffer allocated by this rank.
    // Returns a byte blob (serialised as int32_t array for Python compat).
    // Returns empty vector if IPC is not needed (e.g. fabric memory).
    virtual std::vector<int32_t> exportIpcHandle(void* ptr) = 0;

    // Import peer IPC handles and populate the device-visible tables.
    // remote_handles[i] is the handle exported by rank i (may be empty for
    // ranks that use fabric memory or are on a different node).
    // active_ranks_mask[i] == 1 means rank i is participating.
    // After this call, availableTablePtr() and peerPtrsTablePtr() are valid.
    virtual void importPeerHandles(
        void* local_ptr, int rank, int num_ranks,
        const std::vector<std::vector<int32_t>>& remote_handles,
        const std::vector<int>& active_ranks_mask) = 0;

    // Device pointer to int32_t[num_ranks]: 1 if P2P is available to rank i.
    virtual int32_t* availableTablePtr() = 0;

    // Device pointer to void*[num_ranks]: peer GDR buffer base pointers.
    virtual void** peerPtrsTablePtr() = 0;

    // True if all active ranks have P2P access (fast-path condition).
    virtual bool allPeersAccessible() const = 0;
};

// ---------------------------------------------------------------------------
// RdmaLocalMetadata — exchanged between ranks during IBGDA bootstrap.
// ---------------------------------------------------------------------------
struct RdmaLocalMetadata {
    int64_t raddr;
    int32_t rkey;
    int64_t subnet_prefix;
    int64_t interface_id;
    std::vector<int32_t> qpns;
    std::vector<int32_t> lids;
};

// ---------------------------------------------------------------------------
// RdmaTransport
//
// Manages IBGDA QP lifecycle, MR registration, and the device-visible RDMA
// context tables (raddrs, rkeys, qp_devctxs) used by the EP kernel.
// ---------------------------------------------------------------------------
class RdmaTransport {
   public:
    virtual ~RdmaTransport() = default;

    // Initialise the RDMA transport for the given NIC.
    // device_name: e.g. "mlx5_1".  Pass empty string for auto-detect.
    // Returns 0 on success, non-zero on failure (IBGDA disabled).
    virtual int initialize(const std::string& device_name, int num_ranks,
                           int num_qps) = 0;

    // Register the GDR buffer for RDMA access.
    virtual int registerMemory(void* ptr, size_t bytes) = 0;

    // Allocate the GPU-side control buffer (QP/CQ structures).
    virtual int allocateControlBuffer() = 0;

    // Create QPs in RST→INIT state.  Call after allocateControlBuffer.
    // stream is a cudaStream_t / musaStream_t cast to void*.
    virtual int createQueuePairs(void* stream) = 0;

    // Destroy and recreate QPs (used when active_ranks changes).
    virtual int recreateQueuePairs(void* stream) = 0;

    // Connect QPs to peers using exchanged metadata.
    // is_roce: true for RoCE, false for IB.
    virtual int connectPeers(
        bool is_roce, const std::vector<int64_t>& remote_addrs,
        const std::vector<int32_t>& remote_keys,
        const std::vector<int32_t>& remote_qpns,
        const std::vector<int32_t>& remote_lids,
        const std::vector<int64_t>& subnet_prefixes,
        const std::vector<int64_t>& interface_ids,
        const std::vector<int>& active_ranks_mask) = 0;

    // Metadata for this rank, to be exchanged with peers.
    virtual RdmaLocalMetadata localMetadata() const = 0;

    // Device pointers to the tables consumed by the EP kernel.
    virtual void* raddrsPtr() = 0;   // uint64_t[num_ranks]
    virtual void* rkeysPtr() = 0;    // uint32_t[num_ranks]
    virtual void* qpDevCtxsPtr() = 0;  // mlx5gda_qp_devctx[num_qps]

    virtual bool isRoce() const = 0;
    virtual int gidIndex() const = 0;
};

// ---------------------------------------------------------------------------
// Factory functions — implemented in device_transport.cpp.
// Returns nullptr if the transport is not available on this platform.
// ---------------------------------------------------------------------------

// Create the platform-native P2P transport (NVLink on CUDA, MTLink on MUSA).
std::unique_ptr<P2pTransport> createP2pDeviceTransport(int num_ranks);

// Create the IBGDA RDMA transport backed by TE's RdmaContext.
// device_filter: optional whitelist of NIC names (e.g. {"mlx5_1", "mlx5_2"}).
//   Empty vector = auto-detect via TE's Topology::discover() with no filter.
//   Non-empty = restrict discovery to these NICs, then pick the closest one.
std::unique_ptr<RdmaTransport> createIbgdaDeviceTransport(
    const std::vector<std::string>& device_filter = {});

}  // namespace device
}  // namespace mooncake

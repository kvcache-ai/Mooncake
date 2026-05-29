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

#ifndef TENT_RUNTIME_DEVICE_TRANSPORT_H_
#define TENT_RUNTIME_DEVICE_TRANSPORT_H_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/device_resources.h"

namespace mooncake {
namespace tent {

/// Unified device transport interface for GPU-initiated communication.
///
/// EP (and other consumers) call this interface to set up and manage
/// GPU-kernel-visible communication resources.  Each transport implementation
/// (IBGDA, NVLink, MTLink, CPU proxy, …) inherits from this class and
/// decides how to satisfy each method for its hardware.
///
/// Design principle: EP should never need to know *which* transport it is
/// using.  All platform-specific logic lives inside the implementation.
class DeviceTransport {
   public:
    virtual ~DeviceTransport() = default;

    // -----------------------------------------------------------------------
    // Capabilities
    // -----------------------------------------------------------------------

    /// Returns the capability flags for this transport.
    virtual const DeviceCommCapabilities deviceCapabilities() const = 0;

    // -----------------------------------------------------------------------
    // GPU buffer allocation
    // -----------------------------------------------------------------------

    /// Allocate a GPU buffer suitable for this transport.
    /// For RDMA transports this may register the buffer for RDMA access.
    /// For P2P transports this is a plain GPU malloc.
    /// Fabric-memory transports (MNNVL) use cuMemCreate-style allocation.
    virtual Status allocateBuffer(void** ptr, size_t size,
                                  bool allow_fabric = true) = 0;

    /// Free a buffer allocated by allocateBuffer.
    virtual Status freeBuffer(void* ptr) = 0;

    // -----------------------------------------------------------------------
    // P2P peer setup (NVLink / MTLink / XGMI)
    // -----------------------------------------------------------------------

    /// Allocate per-rank peer access tables (available[], peer_ptrs[]).
    /// No-op for RDMA-only transports.
    virtual Status allocatePeerAccessTables(int rank, int num_ranks) = 0;

    /// Export an IPC handle for a local GPU buffer so remote ranks can access
    /// it.  The handle is represented as int32_t words for language-binding
    /// compatibility.  Returns an empty vector if IPC is not needed (e.g.
    /// fabric memory).
    virtual Status exportIpcHandle(int device_id, void* local_buffer,
                                   std::vector<int32_t>& handle_words) = 0;

    /// Import remote IPC handles and configure peer access.
    /// `remote_handles[i]` is the handle from rank i.
    /// `active_ranks_mask[i]` is non-zero if rank i is active.
    virtual Status configurePeers(
        int local_device_id, void* local_buffer,
        const std::vector<std::vector<int32_t>>& remote_handles,
        const std::vector<int>& active_ranks_mask) = 0;

    /// Whether all peers are accessible via P2P.
    virtual bool allPeersAccessible() const = 0;

    // -----------------------------------------------------------------------
    // RDMA / IBGDA setup
    // -----------------------------------------------------------------------

    /// Initialize the RDMA device.  No-op for P2P-only transports.
    virtual Status initializeRdmaDevice(const std::string& device_name,
                                        uint8_t port_num = 1) = 0;

    /// Register a GPU buffer for RDMA access.  lkey/rkey are set only for
    /// RDMA transports; P2P transports return 0.
    virtual Status registerMemory(void* ptr, size_t size, uint32_t& lkey,
                                  uint32_t& rkey) = 0;

    /// Unregister a previously registered buffer.  No-op for P2P transports.
    virtual Status unregisterMemory(void* ptr) = 0;

    /// Allocate the RDMA control buffer (WQ/CQ/DBR space).
    /// No-op for P2P-only transports.
    virtual Status allocateControlBuffer(size_t size) = 0;

    /// Release the control buffer.  No-op for P2P-only transports.
    virtual Status releaseControlBuffer() = 0;

    /// Pointer to the control buffer (GPU-visible).  nullptr if not
    /// applicable.
    virtual void* controlBuffer() const = 0;

    /// Create RDMA queue pairs.  No-op for P2P-only transports.
    /// `stream` is an opaque compute stream (cudaStream_t / musaStream_t).
    /// `qp_devctxs` points to a pre-allocated GPU buffer for QP device
    /// contexts.
    virtual Status createQueuePairs(int num_qps, int wqe, void* stream,
                                    void* qp_devctxs) = 0;

    /// Recreate queue pairs (e.g. after link change).  No-op for P2P.
    virtual Status recreateQueuePairs(int num_qps, int wqe, void* stream,
                                      void* qp_devctxs) = 0;

    /// Destroy queue pairs.  No-op for P2P-only transports.
    virtual Status destroyQueuePairs() = 0;

    /// Connect to remote RDMA peers.  No-op for P2P-only transports.
    virtual Status connectRdmaPeers(const RdmaPeerConnectInfo& info) = 0;

    // -----------------------------------------------------------------------
    // Metadata accessors
    // -----------------------------------------------------------------------

    /// Local RDMA metadata (raddr, rkey, QPNs, LIDs, GID).  Returns
    /// zeroed/default values for P2P-only transports.
    virtual IbGdaLocalMetadata localMetadata() const = 0;

    /// Whether the RDMA link is RoCE (vs native IB).  False for P2P.
    virtual bool isRoce() const = 0;

    /// GID index for RoCE.  -1 if not applicable.
    virtual int gidIndex() const = 0;

    /// Whether IBGDA/RDMA initialization has been disabled (e.g. no IB
    /// device found).  True for P2P-only transports.
    virtual bool ibgdaDisabled() const = 0;

    // -----------------------------------------------------------------------
    // GPU-kernel-visible context
    // -----------------------------------------------------------------------

    /// Return an opaque pointer to the GPU-visible device context struct.
    /// The caller copies this to GPU memory and passes it to kernels.
    /// The concrete type depends on the transport:
    ///   IBGDA  → IbGdaDeviceContext
    ///   NVLink → NvLinkDeviceContext
    ///   MTLink → MtLinkDeviceContext
    /// The caller uses deviceContextSize() to allocate GPU memory and
    /// deviceContextAbi() to identify the struct type.
    virtual const void* deviceContextPtr() const = 0;

    /// Size of the device context struct in bytes.
    virtual size_t deviceContextSize() const = 0;

    /// ABI tag identifying the device context struct type.
    /// Callers switch on this to interpret deviceContextPtr().
    enum class DeviceContextAbi : uint32_t {
        kNone = 0,
        kIbGda = 1,
        kNvLink = 2,
        kMtLink = 3,
        kP2P = 4,
    };

    virtual DeviceContextAbi deviceContextAbi() const = 0;

    // -----------------------------------------------------------------------
    // GPU-kernel-visible tables (P2P)
    // -----------------------------------------------------------------------

    /// GPU-side P2P availability table: available[rank] == 1 iff that rank's
    /// buffer is reachable via P2P.  nullptr for RDMA-only transports.
    virtual int32_t* availableTablePtr() const = 0;

    /// GPU-side peer pointer table: peer_ptrs[rank] is the device pointer
    /// to rank's buffer (or nullptr if not P2P-reachable).  nullptr for
    /// RDMA-only transports.
    virtual void** peerPtrsTablePtr() const = 0;

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    /// Host-side peer pointer table (for debugging / testing).
    /// Returns nullptr if not applicable.
    virtual void** hostPeerPtrs() const = 0;

    /// Compute the remote pointer for a local pointer + destination rank.
    /// Used by P2P transports.  Returns nullptr for RDMA transports.
    virtual void* getRemotePtr(void* local_ptr, int dst_rank) = 0;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_DEVICE_TRANSPORT_H_

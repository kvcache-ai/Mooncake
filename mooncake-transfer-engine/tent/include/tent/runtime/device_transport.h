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

/// Core device transport interface — buffer allocation, capabilities, and
/// GPU-kernel-visible context.  Every transport implements this.
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
    virtual Status allocateBuffer(void** ptr, size_t size,
                                  bool allow_fabric = true) = 0;

    /// Free a buffer allocated by allocateBuffer.
    virtual Status freeBuffer(void* ptr) = 0;

    /// Whether the last allocateBuffer call used fabric memory (MNNVL).
    virtual bool usesFabricMemory() const { return false; }

    // -----------------------------------------------------------------------
    // GPU-kernel-visible context
    // -----------------------------------------------------------------------

    /// ABI tag identifying the device context struct type.
    enum class DeviceContextAbi : uint32_t {
        kNone = 0,
        kIbGda = 1,
        kNvLink = 2,
        kMtLink = 3,
        kP2P = 4,
    };

    virtual const void* deviceContextPtr() const = 0;
    virtual size_t deviceContextSize() const = 0;
    virtual DeviceContextAbi deviceContextAbi() const = 0;
};

/// P2P transport interface — IPC, peer tables, P2P addressing.
/// Implemented by NVLink, MTLink, and similar interconnects.
class P2pTransport : public DeviceTransport {
   public:
    // -----------------------------------------------------------------------
    // P2P peer setup
    // -----------------------------------------------------------------------

    virtual Status allocatePeerAccessTables(int rank, int num_ranks) = 0;

    virtual Status exportIpcHandle(int device_id, void* local_buffer,
                                   std::vector<int32_t>& handle_words) = 0;

    virtual Status configurePeers(
        int local_device_id, void* local_buffer,
        const std::vector<std::vector<int32_t>>& remote_handles,
        const std::vector<int>& active_ranks_mask) = 0;

    virtual bool allPeersAccessible() const = 0;

    // -----------------------------------------------------------------------
    // GPU-kernel-visible tables
    // -----------------------------------------------------------------------

    virtual int32_t* availableTablePtr() const = 0;
    virtual void** peerPtrsTablePtr() const = 0;

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    virtual void** hostPeerPtrs() const = 0;
    virtual void* getRemotePtr(void* local_ptr, int dst_rank) = 0;
};

/// Device-side RDMA transport interface — MR, QP, control buffer, RDMA peers.
/// Implemented by IBGDA and similar GPU-initiated RDMA transports.
///
/// Named DeviceRdmaTransport to distinguish from the host-side
/// RdmaTransport : Transport (CPU-initiated RDMA bulk transfers).
class DeviceRdmaTransport : public DeviceTransport {
   public:
    // -----------------------------------------------------------------------
    // RDMA device setup
    // -----------------------------------------------------------------------

    virtual Status initializeRdmaDevice(const std::string& device_name,
                                        uint8_t port_num = 1) = 0;

    virtual Status registerMemory(void* ptr, size_t size, uint32_t& lkey,
                                  uint32_t& rkey) = 0;

    virtual Status unregisterMemory(void* ptr) = 0;

    // -----------------------------------------------------------------------
    // Control buffer
    // -----------------------------------------------------------------------

    virtual Status allocateControlBuffer(size_t size) = 0;
    virtual Status releaseControlBuffer() = 0;
    virtual void* controlBuffer() const = 0;

    // -----------------------------------------------------------------------
    // Queue pairs
    // -----------------------------------------------------------------------

    virtual Status createQueuePairs(int num_qps, int wqe, void* stream,
                                    void* qp_devctxs) = 0;

    virtual Status recreateQueuePairs(int num_qps, int wqe, void* stream,
                                      void* qp_devctxs) = 0;

    virtual Status destroyQueuePairs() = 0;

    // -----------------------------------------------------------------------
    // RDMA peer connection
    // -----------------------------------------------------------------------

    virtual Status connectRdmaPeers(const RdmaPeerConnectInfo& info) = 0;

    // -----------------------------------------------------------------------
    // Metadata accessors
    // -----------------------------------------------------------------------

    virtual IbGdaLocalMetadata localMetadata() const = 0;
    virtual bool isRoce() const = 0;
    virtual int gidIndex() const = 0;
    virtual bool ibgdaDisabled() const = 0;
};

/// Auto-detect factory: create the P2P transport for the current platform
/// (NVLink on CUDA, MTLink on MUSA).
std::unique_ptr<P2pTransport> createP2pDeviceTransport();

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RUNTIME_DEVICE_TRANSPORT_H_

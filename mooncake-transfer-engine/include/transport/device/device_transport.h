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

// EP DeviceTransport — host-side abstractions for device-initiated
// communication paths:
//
//   P2pTransport   — intra-node GPU-initiated P2P (NVLink on CUDA, MTLink on
//                    MUSA).  Manages IPC handle exchange and peer pointer
//                    table.
//   RdmaTransport  — inter-node GPU-initiated RDMA (IBGDA / mlx5gda).
//                    Manages QP lifecycle, MR, and device context table.
//   NcclTransport  — optional CUDA LSA / GIN backend. Manages NCCL host and
//                    device communicators plus symmetric windows.
//
// EP code includes only this header and calls the abstract interface.
// Platform-specific implementations live in device/ and are
// selected at build time by the factory functions at the bottom of this file.
//
// The header intentionally avoids including cuda_alike.h so it can be included
// from pure C++ translation units.  Implementations include cuda_alike.h.

#include <cstddef>
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

    // Verify that peer-mapped memory is writable (via memcpy).
    // Returns true if a small test write to each peer's mapped buffer
    // succeeds.  On failure, sets all_peers_accessible to false.
    virtual bool verifyPeerAccess() = 0;
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
    virtual int connectPeers(int local_rank, bool is_roce,
                             const std::vector<int64_t>& remote_addrs,
                             const std::vector<int32_t>& remote_keys,
                             const std::vector<int32_t>& remote_qpns,
                             const std::vector<int32_t>& remote_lids,
                             const std::vector<int64_t>& subnet_prefixes,
                             const std::vector<int64_t>& interface_ids,
                             const std::vector<int>& active_ranks_mask) = 0;

    // Metadata for this rank, to be exchanged with peers.
    virtual RdmaLocalMetadata localMetadata() const = 0;

    // Device pointers to the tables consumed by the EP kernel.
    virtual void* raddrsPtr() = 0;     // uint64_t[num_ranks]
    virtual void* rkeysPtr() = 0;      // uint32_t[num_ranks]
    virtual void* qpDevCtxsPtr() = 0;  // mlx5gda_qp_devctx[num_qps]

    virtual bool isRoce() const = 0;
    virtual int gidIndex() const = 0;
};

#ifdef USE_NCCL_DEVICE
// ---------------------------------------------------------------------------
// NcclTransport
//
// Owns the native NCCL resources used to implement Mooncake's LSA/GIN device
// operations. Native communicators and windows are intentionally private.
// Communicator bootstrap and buffer registration are collective; the caller
// supplies the control plane used to exchange the unique ID and to order
// collectives.
// ---------------------------------------------------------------------------
enum class NcclGinBackend : uint8_t {
    kNone = 0,
    kProxy,
    kGdaki,
};

enum class NcclDeviceRoute : uint8_t {
    kUnavailable = 0,
    kLocal,
    kLsa,
    kGin,
};

struct NcclTransportConfig {
    int rank = -1;
    int num_ranks = 0;

    // Mooncake currently supports either full world-team GIN connectivity or
    // no GIN. Rail connectivity is intentionally deferred until Mooncake has
    // a rail-team rank contract.
    bool enable_gin = true;
    int gin_context_count = 4;
    bool gin_exclusive_contexts = false;

    // LSA barriers synchronize only the local LSA team. Cross-LSA/world
    // synchronization remains the caller's responsibility.
    int lsa_barrier_count = 0;
    bool require_lsa_multimem = false;
};

struct NcclTransportProperties {
    int runtime_version = 0;
    int rank = -1;
    int num_ranks = 0;
    int cuda_device = -1;
    bool device_api_supported = false;
    bool multimem_supported = false;
    bool lsa_multimem_enabled = false;
    int lsa_team_count = 0;
    int lsa_barrier_count = 0;
    bool gin_enabled = false;
    NcclGinBackend gin_backend = NcclGinBackend::kNone;
    int gin_connection_count = 0;
    int gin_context_count = 0;
};

namespace detail {
struct NcclDeviceContextAccess;
}  // namespace detail

class NcclDeviceTransportImpl;

// Opaque token for one collectively registered symmetric buffer. The token
// does not own the allocation; deregister it before freeing the buffer.
class NcclBufferRegistration {
   public:
    bool valid() const { return id_ != 0; }

   private:
    uint64_t id_ = 0;

    friend class NcclDeviceTransportImpl;
};

// Pass this small Mooncake context by value to kernels. Its native NCCL
// communicator and registration table remain behind an opaque device pointer.
class NcclDeviceContext {
   public:
    bool valid() const { return native_comm_ != nullptr; }

   private:
    const void* native_comm_ = nullptr;
    const void* native_window_ = nullptr;
    const void* local_base_ = nullptr;
    size_t buffer_bytes_ = 0;
    int rank_ = -1;
    int num_ranks_ = 0;
    int gin_context_count_ = 0;
    int lsa_barrier_count_ = 0;
    bool gin_enabled_ = false;
    bool lsa_multimem_enabled_ = false;

    friend class NcclDeviceTransportImpl;
    friend struct detail::NcclDeviceContextAccess;
};

// Host calls on one transport instance are not thread-safe and must be
// externally serialized, matching P2pTransport and RdmaTransport.
class NcclTransport {
   public:
    virtual ~NcclTransport() = default;

    // Generate the NCCL bootstrap ID on one rank. Exchange this int32_t blob
    // through the caller's existing control plane before initialize().
    virtual std::vector<int32_t> createUniqueId() = 0;

    // Create the host and device communicators. Every rank must call this with
    // the same unique ID and compatible config.
    virtual int initialize(const NcclTransportConfig& config,
                           const std::vector<int32_t>& unique_id) = 0;

    // NCCL-compatible VMM allocation. These low-level methods are local; every
    // rank must verify allocation success before entering registerBuffer().
    virtual void* allocateBuffer(size_t bytes) = 0;
    virtual int freeBuffer(void* ptr) = 0;

    // Collectively register a symmetric buffer. Calls must occur in the same
    // order on every rank. Deregistration is local after all device work and
    // remote access have completed. The registration is invalidated on
    // successful deregistration.
    virtual int registerBuffer(void* ptr, size_t bytes,
                               NcclBufferRegistration* registration) = 0;
    virtual int deregisterBuffer(
        NcclBufferRegistration* registration) = 0;

    // Safe common path: every rank allocates, collectively checks that all
    // allocations succeeded, and only then enters registration. All ranks
    // must call this method in the same order. On failure, successful local
    // allocations and registrations are cleaned up before returning.
    virtual int allocateAndRegisterBuffer(
        size_t bytes, void** ptr,
        NcclBufferRegistration* registration) = 0;

    // Snapshot passed by value to a CUDA kernel and bound to one registered
    // buffer. Device helpers accept local pointers within that buffer and
    // resolve the private window and peer offsets. The snapshot remains valid
    // until the registration is removed or the transport is shut down.
    virtual NcclDeviceContext deviceContext(
        const NcclBufferRegistration& registration) const = 0;

    virtual NcclTransportProperties properties() const = 0;
    virtual bool initialized() const = 0;

    // The caller must first ensure that no kernel can access the context or
    // any registered buffer.
    virtual int shutdown() = 0;
};
#endif

// ---------------------------------------------------------------------------
// Factory functions — implemented under src/transport/device/.
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

#ifdef USE_NCCL_DEVICE
// Create the CUDA-only NCCL LSA/GIN device transport.
std::unique_ptr<NcclTransport> createNcclDeviceTransport();
#endif

}  // namespace device
}  // namespace mooncake

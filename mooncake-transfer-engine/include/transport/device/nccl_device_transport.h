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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace mooncake {
namespace device {

enum class NcclGinBackend : uint8_t {
    kNone = 0,
    kProxy,
    kGdaki,
    kGpi,
};

enum class NcclDeviceRoute : uint8_t {
    kUnavailable = 0,
    kLocal,
    kLsa,
    kGin,
};

enum class NcclGinConnectionType : uint8_t {
    kNone = 0,
    kFull,
    kRail,
};

struct NcclTransportConfig {
    int rank = -1;
    int num_ranks = 0;

    // enable_gin=false overrides gin_connection_type. Rail connectivity uses
    // NCCL's rail team: peers are addressed by their rail-team rank rather
    // than by world rank.
    bool enable_gin = true;
    NcclGinConnectionType gin_connection_type = NcclGinConnectionType::kFull;
    int gin_context_count = 4;
    bool gin_exclusive_contexts = false;
    int gin_queue_depth = 0;
    int gin_signal_count = 0;
    // A negative value leaves NCCL's default traffic class unchanged.
    int gin_traffic_class = -1;

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
    NcclGinConnectionType gin_connection_type = NcclGinConnectionType::kNone;
    NcclGinBackend gin_backend = NcclGinBackend::kNone;
    int gin_connection_count = 0;
    int gin_context_count = 0;
};

// Coordinates of this rank in its contiguous LSA team. Kept separate from
// NcclTransportProperties so extending the experimental API does not change
// the size of the existing by-value properties return type.
struct NcclLsaTopology {
    int rank = -1;
    int size = 0;
    int first_rank = -1;
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

// Pass this small Mooncake context by value to kernels. Native NCCL
// communicators and windows remain behind opaque pointers.
class NcclDeviceContext {
   public:
    // ncclDevComm is embedded so kernels read its hot fields from parameter
    // memory instead of chasing a global-memory pointer. Keep a small amount of
    // headroom because the version-specific NCCL structure can grow between
    // releases; nccl_device.cuh verifies that the headers in use still fit.
    static constexpr size_t kNativeCommCapacity = 256;
    static constexpr size_t kNativeCommAlignment = 8;

    bool valid() const { return native_comm_ != nullptr; }

   private:
    const void* native_comm_ = nullptr;
    alignas(kNativeCommAlignment) unsigned char native_comm_storage_
        [kNativeCommCapacity]{};
    const void* native_window_ = nullptr;
    const void* local_base_ = nullptr;
    int rank_ = -1;
    int gin_context_count_ = 0;
    bool gin_enabled_ = false;
    bool gin_connections_railed_ = false;
    bool lsa_multimem_enabled_ = false;

    friend class NcclDeviceTransportImpl;
    friend struct detail::NcclDeviceContextAccess;
};

// Host calls on one transport instance are not thread-safe and must be
// externally serialized. initialize() binds the transport to the current CUDA
// device; keep that device current for every later host call, including
// shutdown and destruction.
//
class NcclTransport {
   public:
    virtual ~NcclTransport() = default;

    // Generate the NCCL bootstrap ID on one rank. Exchange this int32_t blob
    // through the caller's existing control plane before initialize().
    virtual std::vector<int32_t> createUniqueId() = 0;

    // Create the host and device communicators. Every rank must call this with
    // the same unique ID and compatible config. The NCCL headers used to build
    // Mooncake and device kernels must exactly match the runtime libnccl;
    // initialization rejects a mismatch. Rebuild AOT kernels and regenerate
    // cached JIT kernels after every NCCL upgrade.
    virtual int initialize(const NcclTransportConfig& config,
                           const std::vector<int32_t>& unique_id) = 0;

    // NCCL-compatible VMM allocation. These low-level methods are local; every
    // rank must verify allocation success before entering registerBuffer().
    virtual void* allocateBuffer(size_t bytes) = 0;
    virtual int freeBuffer(void* ptr) = 0;

    // Collectively register a symmetric, strictly ordered NCCL window. Calls
    // must occur in the same order on every rank. Every pointer passed to a
    // GIN helper through the returned context must be wholly contained in this
    // buffer. Deregistration is local after all device work and remote access
    // have completed. The registration is invalidated on successful
    // deregistration.
    virtual int registerBuffer(void* ptr, size_t bytes,
                               NcclBufferRegistration* registration) = 0;
    virtual int deregisterBuffer(NcclBufferRegistration* registration) = 0;

    // Coordinated common path: every rank allocates and collectively checks all
    // allocations, zero-initializes the allocation before it can contain GIN
    // VA signals, and only then enters registration. All ranks must call this
    // method in the same order.
    virtual int allocateAndRegisterBuffer(
        size_t bytes, void** ptr, NcclBufferRegistration* registration) = 0;

    // Snapshot passed by value to a CUDA kernel and bound to one registered
    // buffer. It remains valid until registration removal or shutdown.
    virtual NcclDeviceContext deviceContext(
        const NcclBufferRegistration& registration) const = 0;

    virtual NcclTransportProperties properties() const = 0;
    virtual bool initialized() const = 0;

    // The caller must first ensure that no kernel can access the context or
    // any registered buffer, then call shutdown in coordinated order on every
    // communicator rank. Rank-local destructor cleanup is not a substitute for
    // coordinated NCCL communicator teardown.
    virtual int shutdown() = 0;

    // Host-side collective status agreement. Every rank must call this in the
    // same order after initialize(), while the CUDA stream and NCCL
    // communicator remain healthy. It returns true only when local_success is
    // true on every communicator rank. This coordinates application status; it
    // is not recovery from a CUDA/NCCL control-path failure. Use it before any
    // rank throws from a collectively constructed object. Kept at the end to
    // preserve the vtable slots of the interface introduced by the initial
    // NCCL backend.
    virtual bool allRanksSucceeded(bool local_success) = 0;
    virtual NcclLsaTopology lsaTopology() const = 0;
};

// Create the CUDA-only NCCL LSA/GIN device transport.
std::unique_ptr<NcclTransport> createNcclDeviceTransport();

}  // namespace device
}  // namespace mooncake
